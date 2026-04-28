//! Microbenchmarks for the text search hot path.
//!
//! Run with: `cargo bench -p cairn-text`.

use cairn_place::{Coord, LocalizedName, Place, PlaceId, PlaceKind};
use cairn_text::{build_index, SearchMode, SearchOptions, TextIndex};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn synthetic_places(n: usize) -> Vec<Place> {
    let names = [
        "Vaduz",
        "Schaan",
        "Triesen",
        "Balzers",
        "Eschen",
        "Mauren",
        "Gamprin",
        "Ruggell",
        "Triesenberg",
        "Planken",
    ];
    (0..n)
        .map(|i| {
            let base = names[i % names.len()];
            let id = PlaceId::new(1, (i / 1024) as u32, (i % 1024) as u64).unwrap();
            Place {
                id,
                kind: PlaceKind::City,
                names: vec![LocalizedName {
                    lang: "default".into(),
                    value: format!("{base}-{i}"),
                }],
                centroid: Coord {
                    lon: 9.0 + (i as f64).sin(),
                    lat: 47.0 + (i as f64).cos(),
                },
                admin_path: vec![],
                tags: vec![],
            }
        })
        .collect()
}

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let d = std::env::temp_dir().join(format!(
        "cairn-text-bench-{}-{}-{}",
        std::process::id(),
        nanos,
        n
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn bench_search(c: &mut Criterion) {
    let dir = tempdir();
    let _ = build_index(&dir, synthetic_places(5_000)).unwrap();
    let idx = TextIndex::open(&dir).unwrap();

    c.bench_function("search_exact_5k", |b| {
        b.iter(|| {
            let _ = idx
                .search(black_box("Vaduz-42"), &SearchOptions::default())
                .unwrap();
        })
    });

    let opts = SearchOptions {
        mode: SearchMode::Autocomplete,
        ..Default::default()
    };
    c.bench_function("autocomplete_3char_5k", |b| {
        b.iter(|| {
            let _ = idx.search(black_box("Vad"), &opts).unwrap();
        })
    });

    let fuzzy_opts = SearchOptions {
        fuzzy: 2,
        ..Default::default()
    };
    c.bench_function("fuzzy_typo_5k", |b| {
        b.iter(|| {
            let _ = idx.search(black_box("vaaduz"), &fuzzy_opts).unwrap();
        })
    });
}

criterion_group!(benches, bench_search);
criterion_main!(benches);
