//! Ed25519 bundle signing + verification.
//!
//! Cairn signs the bundle's `manifest.toml` (which already carries
//! blake3 hashes for every tile + admin + point + text artifact),
//! producing a detached signature at `manifest.toml.sig`. Operators
//! pin the signing public key — anyone with that key can verify a
//! bundle is exactly what the builder shipped, even after it travels
//! through an airgap or third-party mirror.
//!
//! Why ed25519: 32-byte keys, 64-byte signatures, no curve params, no
//! HSM dependency, fast verify, supported in OpenSSH / age / sigstore.
//! Pure-Rust via `ed25519-dalek` so the static-musl build stays
//! self-contained.
//!
//! On-disk format:
//!   - `cairn.key`           — 32-byte raw secret (binary). Mode 0600.
//!   - `cairn.pub`           — 32-byte raw public key (binary).
//!   - `<bundle>/manifest.toml.sig` — 64-byte raw signature (binary).
//!
//! Operators who need a richer transport (PEM, JWK, sigstore bundle)
//! can layer their own envelope on top — the raw bytes here keep the
//! airgap-friendly path simple.

use anyhow::{Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey, SECRET_KEY_LENGTH};
use rand::rngs::OsRng;
use std::path::{Path, PathBuf};

const SIG_FILE: &str = "manifest.toml.sig";
const PUB_FILE: &str = "cairn.pub";
const KEY_FILE: &str = "cairn.key";

/// Generate a fresh ed25519 keypair and write `cairn.key` (secret) +
/// `cairn.pub` (public) into `out_dir`. Refuses to overwrite an
/// existing `cairn.key` so a stray rerun can't silently swap an
/// already-deployed key.
pub fn cmd_keygen(out_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(out_dir)
        .with_context(|| format!("creating key dir {}", out_dir.display()))?;
    let key_path = out_dir.join(KEY_FILE);
    let pub_path = out_dir.join(PUB_FILE);
    if key_path.exists() {
        return Err(anyhow::anyhow!(
            "{} already exists — refuse to overwrite",
            key_path.display()
        ));
    }

    let signing = SigningKey::generate(&mut OsRng);
    let verifying = signing.verifying_key();
    write_secret(&key_path, signing.to_bytes().as_slice())?;
    std::fs::write(&pub_path, verifying.to_bytes())
        .with_context(|| format!("writing {}", pub_path.display()))?;

    println!("wrote secret key  : {} (mode 0600)", key_path.display());
    println!("wrote public key  : {}", pub_path.display());
    println!(
        "public key (hex)  : {}",
        hex(verifying.to_bytes().as_slice())
    );
    Ok(())
}

/// Sign `<bundle>/manifest.toml` with the secret at `key_path`.
/// Writes the detached signature to `<bundle>/manifest.toml.sig`.
pub fn cmd_sign(bundle: &Path, key_path: &Path) -> Result<PathBuf> {
    let manifest = bundle.join("manifest.toml");
    if !manifest.exists() {
        return Err(anyhow::anyhow!(
            "manifest missing at {} — build the bundle first",
            manifest.display()
        ));
    }
    let signing = read_secret(key_path)?;
    let body =
        std::fs::read(&manifest).with_context(|| format!("reading {}", manifest.display()))?;
    let sig: Signature = signing.sign(&body);
    let sig_path = bundle.join(SIG_FILE);
    std::fs::write(&sig_path, sig.to_bytes())
        .with_context(|| format!("writing {}", sig_path.display()))?;
    println!("signed manifest  : {}", manifest.display());
    println!("signature        : {}", sig_path.display());
    println!(
        "public key (hex) : {}",
        hex(signing.verifying_key().to_bytes().as_slice())
    );
    Ok(sig_path)
}

/// Verify `<bundle>/manifest.toml` against `<bundle>/manifest.toml.sig`
/// using the public key at `pub_path`. Returns `Ok(())` only when the
/// signature checks out; anything else is a hard error so CI / deploy
/// pipelines can fail loudly.
pub fn cmd_verify(bundle: &Path, pub_path: &Path) -> Result<()> {
    let manifest = bundle.join("manifest.toml");
    let sig_path = bundle.join(SIG_FILE);
    if !manifest.exists() {
        return Err(anyhow::anyhow!(
            "manifest missing at {}",
            manifest.display()
        ));
    }
    if !sig_path.exists() {
        return Err(anyhow::anyhow!(
            "signature missing at {} — run `cairn-build sign` first",
            sig_path.display()
        ));
    }
    let verifying = read_public(pub_path)?;
    let body = std::fs::read(&manifest)?;
    let sig_bytes = std::fs::read(&sig_path)?;
    if sig_bytes.len() != 64 {
        return Err(anyhow::anyhow!(
            "signature at {} is {} bytes; expected 64",
            sig_path.display(),
            sig_bytes.len()
        ));
    }
    let mut sig_arr = [0u8; 64];
    sig_arr.copy_from_slice(&sig_bytes);
    let sig = Signature::from_bytes(&sig_arr);
    verifying
        .verify(&body, &sig)
        .map_err(|e| anyhow::anyhow!("signature INVALID: {e}"))?;
    println!(
        "OK signature valid for {} ({} bytes)",
        manifest.display(),
        body.len()
    );
    Ok(())
}

fn read_secret(path: &Path) -> Result<SigningKey> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if bytes.len() != SECRET_KEY_LENGTH {
        return Err(anyhow::anyhow!(
            "secret key at {} is {} bytes; expected {SECRET_KEY_LENGTH}",
            path.display(),
            bytes.len()
        ));
    }
    let mut arr = [0u8; SECRET_KEY_LENGTH];
    arr.copy_from_slice(&bytes);
    Ok(SigningKey::from_bytes(&arr))
}

fn read_public(path: &Path) -> Result<VerifyingKey> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if bytes.len() != 32 {
        return Err(anyhow::anyhow!(
            "public key at {} is {} bytes; expected 32",
            path.display(),
            bytes.len()
        ));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    VerifyingKey::from_bytes(&arr).map_err(|e| anyhow::anyhow!("public key parse failed: {e}"))
}

fn write_secret(path: &Path, bytes: &[u8]) -> Result<()> {
    std::fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path)?.permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(path, perms)?;
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "cairn-sign-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn fake_manifest(dir: &Path, body: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("manifest.toml"), body).unwrap();
    }

    #[test]
    fn keygen_then_sign_then_verify_roundtrip() {
        let key_dir = tmpdir();
        cmd_keygen(&key_dir).unwrap();
        assert!(key_dir.join(KEY_FILE).exists());
        assert!(key_dir.join(PUB_FILE).exists());

        let bundle = tmpdir();
        fake_manifest(&bundle, "bundle_id = \"x\"\n");
        cmd_sign(&bundle, &key_dir.join(KEY_FILE)).unwrap();
        assert!(bundle.join(SIG_FILE).exists());

        cmd_verify(&bundle, &key_dir.join(PUB_FILE)).unwrap();
    }

    #[test]
    fn verify_rejects_tampered_manifest() {
        let key_dir = tmpdir();
        cmd_keygen(&key_dir).unwrap();
        let bundle = tmpdir();
        fake_manifest(&bundle, "bundle_id = \"x\"\n");
        cmd_sign(&bundle, &key_dir.join(KEY_FILE)).unwrap();

        // Tamper.
        std::fs::write(bundle.join("manifest.toml"), "bundle_id = \"evil\"\n").unwrap();

        let res = cmd_verify(&bundle, &key_dir.join(PUB_FILE));
        assert!(res.is_err(), "tampered manifest must fail verify");
        let msg = res.unwrap_err().to_string();
        assert!(msg.contains("INVALID"), "expected INVALID in error: {msg}");
    }

    #[test]
    fn verify_rejects_wrong_public_key() {
        let key_dir1 = tmpdir();
        let key_dir2 = tmpdir();
        cmd_keygen(&key_dir1).unwrap();
        cmd_keygen(&key_dir2).unwrap();

        let bundle = tmpdir();
        fake_manifest(&bundle, "x\n");
        cmd_sign(&bundle, &key_dir1.join(KEY_FILE)).unwrap();
        let res = cmd_verify(&bundle, &key_dir2.join(PUB_FILE));
        assert!(res.is_err(), "wrong public key must fail verify");
    }

    #[test]
    fn keygen_refuses_overwrite() {
        let key_dir = tmpdir();
        cmd_keygen(&key_dir).unwrap();
        let res = cmd_keygen(&key_dir);
        assert!(res.is_err(), "second keygen on same dir must fail");
    }

    #[test]
    fn sign_errors_when_manifest_missing() {
        let key_dir = tmpdir();
        cmd_keygen(&key_dir).unwrap();
        let bundle = tmpdir();
        std::fs::create_dir_all(&bundle).unwrap();
        let res = cmd_sign(&bundle, &key_dir.join(KEY_FILE));
        assert!(res.is_err());
    }

    #[test]
    fn verify_errors_when_signature_missing() {
        let key_dir = tmpdir();
        cmd_keygen(&key_dir).unwrap();
        let bundle = tmpdir();
        fake_manifest(&bundle, "x\n");
        let res = cmd_verify(&bundle, &key_dir.join(PUB_FILE));
        assert!(res.is_err());
    }
}
