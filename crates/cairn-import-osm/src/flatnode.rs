//! Flatnode — disk-backed mmap'd node-coord lookup.
//!
//! Phase 6f part 2. Targets continent / planet OSM imports where the
//! `Inline` and `SortedVec` strategies (which keep the cache resident
//! in RAM) blow past commodity host RAM.
//!
//! Layout: a single file with a 16-byte header, followed by a dense
//! `[i32; 2]` array indexed by `node_id` (0-based). Each slot holds
//! `(lon_q, lat_q)` quantized to `degrees * 1e7`. Slot `(0, 0)` is
//! treated as **absent**.
//!
//! ```text
//! [0..8]    : magic "CRNFLAT1"
//! [8..16]   : max_node_id (u64 LE)
//! [16..]    : (lon_q, lat_q) i32 LE × (max_node_id + 1)
//! ```
//!
//! Read path is `O(1)` direct array access. RSS is bounded by the
//! mmap working set — the kernel pages out unused regions, so a
//! 110 GB planet-scale flatnode file resident in only the touched
//! pages stays at a few GB peak.
//!
//! ## Sparse-file design
//!
//! Global OSM node IDs are sparse — even a Switzerland extract has
//! `max_node_id` in the billions. We rely on POSIX sparse-file
//! semantics: `File::set_len(N)` reserves the logical range without
//! allocating disk pages, and only-written slots back to physical
//! storage. A 110 GB logical flatnode file holding 50 M actually-
//! written slots typically allocates ~400 MB of real disk.
//!
//! Sentinel is `(0, 0)` rather than `(i32::MIN, i32::MIN)` precisely
//! to keep the file sparse. Pre-filling with a non-zero sentinel
//! would touch every page and balloon real disk usage to the full
//! logical size, defeating the point.
//!
//! **Null-Island caveat**: a real node at exactly lat=0, lon=0
//! quantizes to `(0, 0)` and is therefore indistinguishable from an
//! unwritten slot. OSM has no nodes at exactly (0, 0); the closest
//! are a few meters offshore in the Gulf of Guinea and they don't
//! lie on any way `cairn-build` would centroid. `osm2pgsql
//! --flat-nodes` makes the same trade-off.

use memmap2::{Mmap, MmapMut};
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

const MAGIC: &[u8; 8] = b"CRNFLAT1";
const HEADER_SIZE: usize = 16;
const SLOT_SIZE: usize = 8;

#[derive(Debug, Error)]
pub enum FlatnodeError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("flatnode header magic mismatch (got {got:?})")]
    BadMagic { got: [u8; 8] },
    #[error("flatnode file truncated: header says max_id={max_id} but file is {actual_len} bytes (expected at least {expected_len})")]
    Truncated {
        max_id: i64,
        actual_len: u64,
        expected_len: u64,
    },
}

#[inline]
fn quantize(lonlat: [f64; 2]) -> [i32; 2] {
    [
        (lonlat[0] * 1e7).round() as i32,
        (lonlat[1] * 1e7).round() as i32,
    ]
}

#[inline]
fn dequantize(q: [i32; 2]) -> [f64; 2] {
    [q[0] as f64 / 1e7, q[1] as f64 / 1e7]
}

/// Total file size for a flatnode storing slots `0..=max_id`.
pub fn flatnode_file_size(max_id: i64) -> u64 {
    HEADER_SIZE as u64 + ((max_id as u64) + 1) * SLOT_SIZE as u64
}

/// Mutable mmap'd flatnode under construction. Set node coords via
/// [`Self::set`], then call [`Self::finalize`] to flush + atomically
/// rename onto the final path.
pub struct FlatnodeWriter {
    /// Path the writer will rename to on finalize.
    final_path: PathBuf,
    /// Live `<final>.tmp` path — written then renamed.
    tmp_path: PathBuf,
    /// Backing file (kept alive while mmap is in use).
    _file: File,
    mmap: MmapMut,
    max_id: i64,
}

impl FlatnodeWriter {
    /// Create a new flatnode at `path` sized to hold slots `0..=max_id`.
    ///
    /// Relies on POSIX sparse-file semantics: only slots written via
    /// [`Self::set`] back to physical disk pages. Slots left
    /// untouched read back as `(0, 0)` and are treated as absent
    /// (see the Null-Island caveat in the module docs).
    pub fn create(path: &Path, max_id: i64) -> Result<Self, FlatnodeError> {
        let final_path = path.to_path_buf();
        let tmp_path = path.with_extension("flatnode.tmp");
        let total = flatnode_file_size(max_id);

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&tmp_path)?;
        file.set_len(total)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        // Header. Body intentionally left at the kernel's zero fill.
        mmap[0..8].copy_from_slice(MAGIC);
        mmap[8..16].copy_from_slice(&(max_id as u64).to_le_bytes());

        Ok(FlatnodeWriter {
            final_path,
            tmp_path,
            _file: file,
            mmap,
            max_id,
        })
    }

    /// Capacity (max writable id, inclusive).
    pub fn max_id(&self) -> i64 {
        self.max_id
    }

    /// Write a quantized coord into the slot for `id`. Caller must
    /// ensure `id` is within `0..=max_id`; out-of-range writes are
    /// dropped (no panic — keeps the parallel writer ergonomic).
    pub fn set(&mut self, id: i64, lonlat: [f64; 2]) {
        if id < 0 || id > self.max_id {
            return;
        }
        let q = quantize(lonlat);
        let off = HEADER_SIZE + (id as usize) * SLOT_SIZE;
        self.mmap[off..off + 4].copy_from_slice(&q[0].to_le_bytes());
        self.mmap[off + 4..off + 8].copy_from_slice(&q[1].to_le_bytes());
    }

    /// Flush the mmap, fsync the file, drop the mapping, and
    /// atomically rename the `<final>.tmp` file onto `<final>`.
    pub fn finalize(self) -> Result<PathBuf, FlatnodeError> {
        self.mmap.flush()?;
        // Drop the mmap before rename to avoid a Windows-style
        // open-handle conflict; macOS / Linux are tolerant but it's
        // cheap to be portable.
        drop(self.mmap);
        let _ = self._file.sync_all();
        std::fs::rename(&self.tmp_path, &self.final_path)?;
        Ok(self.final_path)
    }
}

#[inline]
fn slot_is_sentinel(slot: [i32; 2]) -> bool {
    slot == [0, 0]
}

/// Read-only mmap'd flatnode. Lookup is `O(1)` direct slot access.
/// Drop releases the mapping; the underlying file stays on disk so a
/// subsequent run can reuse it.
pub struct FlatnodeReader {
    _file: File,
    mmap: Mmap,
    max_id: i64,
    path: PathBuf,
}

impl FlatnodeReader {
    /// Open an existing flatnode for read.
    pub fn open(path: &Path) -> Result<Self, FlatnodeError> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        if mmap.len() < HEADER_SIZE {
            return Err(FlatnodeError::BadMagic {
                got: [0u8; 8],
            });
        }
        let mut got = [0u8; 8];
        got.copy_from_slice(&mmap[0..8]);
        if &got != MAGIC {
            return Err(FlatnodeError::BadMagic { got });
        }
        let max_id = i64::from_le_bytes(mmap[8..16].try_into().unwrap());
        let expected = flatnode_file_size(max_id);
        if (mmap.len() as u64) < expected {
            return Err(FlatnodeError::Truncated {
                max_id,
                actual_len: mmap.len() as u64,
                expected_len: expected,
            });
        }
        Ok(FlatnodeReader {
            _file: file,
            mmap,
            max_id,
            path: path.to_path_buf(),
        })
    }

    pub fn max_id(&self) -> i64 {
        self.max_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Total entries the flatnode addresses (max_id + 1). Includes
    /// absent slots — use [`Self::populated_count`] for the
    /// non-sentinel count (linear scan).
    pub fn slot_count(&self) -> u64 {
        (self.max_id as u64).saturating_add(1)
    }

    /// Linear-scan count of non-sentinel slots. O(slot_count); use
    /// only for diagnostics, not on the hot path.
    pub fn populated_count(&self) -> u64 {
        let body = &self.mmap[HEADER_SIZE..];
        let mut n = 0u64;
        for chunk in body.chunks_exact(SLOT_SIZE) {
            let lon_q = i32::from_le_bytes(chunk[0..4].try_into().unwrap());
            let lat_q = i32::from_le_bytes(chunk[4..8].try_into().unwrap());
            if !slot_is_sentinel([lon_q, lat_q]) {
                n += 1;
            }
        }
        n
    }

    /// Lookup. Returns `None` for ids outside `0..=max_id` or for
    /// slots still carrying the absence sentinel.
    #[inline]
    pub fn get(&self, id: i64) -> Option<[f64; 2]> {
        if id < 0 || id > self.max_id {
            return None;
        }
        let off = HEADER_SIZE + (id as usize) * SLOT_SIZE;
        let lon_q = i32::from_le_bytes(self.mmap[off..off + 4].try_into().unwrap());
        let lat_q = i32::from_le_bytes(self.mmap[off + 4..off + 8].try_into().unwrap());
        if slot_is_sentinel([lon_q, lat_q]) {
            None
        } else {
            Some(dequantize([lon_q, lat_q]))
        }
    }

    /// Approximate resident-set cost. The actual RSS depends on the
    /// kernel's mmap working set, which is much smaller than the
    /// file size for sparse workloads.
    pub fn approx_disk_bytes(&self) -> usize {
        self.mmap.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn roundtrip_small() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("flat.bin");

        {
            let mut w = FlatnodeWriter::create(&path, 1000).unwrap();
            w.set(1, [9.5314, 47.3769]); // Liechtenstein-ish
            w.set(500, [-122.4194, 37.7749]); // San Francisco
            w.set(1000, [180.0, 90.0]); // North-pole edge
            w.finalize().unwrap();
        }

        let r = FlatnodeReader::open(&path).unwrap();
        assert_eq!(r.max_id(), 1000);
        assert!(r.get(0).is_none(), "slot 0 must read absent");
        let g1 = r.get(1).unwrap();
        assert!((g1[0] - 9.5314).abs() < 1e-6);
        assert!((g1[1] - 47.3769).abs() < 1e-6);
        assert!(r.get(2).is_none(), "unwritten slot must read absent");
        let g500 = r.get(500).unwrap();
        assert!((g500[0] - -122.4194).abs() < 1e-6);
        let g1000 = r.get(1000).unwrap();
        assert!((g1000[0] - 180.0).abs() < 1e-6);
        assert!(r.get(1001).is_none(), "out-of-range id");
        assert_eq!(r.populated_count(), 3);
    }

    #[test]
    fn rejects_bad_magic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad.bin");
        std::fs::write(&path, b"NOTACAIRN0000000").unwrap();
        match FlatnodeReader::open(&path) {
            Err(FlatnodeError::BadMagic { .. }) => {}
            other => panic!("expected BadMagic, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn rejects_truncated_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("trunc.bin");
        // Valid 16-byte header claiming max_id = 1000, but no body.
        let mut hdr = Vec::with_capacity(HEADER_SIZE);
        hdr.extend_from_slice(MAGIC);
        hdr.extend_from_slice(&(1000_u64).to_le_bytes());
        std::fs::write(&path, &hdr).unwrap();
        match FlatnodeReader::open(&path) {
            Err(FlatnodeError::Truncated { max_id, .. }) => assert_eq!(max_id, 1000),
            other => panic!("expected Truncated, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn quantize_dequantize_lossless() {
        for raw in [0_i32, 1, -1, 1_799_999_999, -1_800_000_000] {
            let lon = raw as f64 / 1e7;
            let lat = (-raw) as f64 / 1e7;
            let back = dequantize(quantize([lon, lat]));
            assert!((back[0] - lon).abs() < 1e-9);
            assert!((back[1] - lat).abs() < 1e-9);
        }
    }

    #[test]
    fn null_island_collides_with_absent_by_design() {
        // Documented design tradeoff: writing (0.0, 0.0) into a slot
        // is indistinguishable from leaving the slot at the
        // sparse-file zero fill, since both quantize to (0, 0). OSM
        // has no nodes at exactly (0, 0); osm2pgsql --flat-nodes
        // accepts the same caveat. This test pins the behavior so a
        // future change away from sparse-file friendliness is loud.
        let dir = tempdir().unwrap();
        let path = dir.path().join("null.bin");
        {
            let mut w = FlatnodeWriter::create(&path, 4).unwrap();
            w.set(2, [0.0, 0.0]);
            w.finalize().unwrap();
        }
        let r = FlatnodeReader::open(&path).unwrap();
        // Slot 2 was written with (0, 0) but reads back as absent —
        // the sentinel is structurally indistinguishable from a real
        // Null-Island coordinate at OSM's quantization scale.
        assert!(r.get(2).is_none(), "Null-Island writes collide with absent");
        assert!(r.get(0).is_none());
        assert!(r.get(1).is_none());
        assert!(r.get(3).is_none());
        assert_eq!(r.populated_count(), 0);
    }
}
