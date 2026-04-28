# Security

This document describes the security posture of Cairn, how vulnerabilities
are tracked, and how to report new ones.

## Reporting a vulnerability

If you believe you have found a security issue in Cairn:

- **Do not open a public GitHub issue.**
- Email **cairn-security@kaldera.dev** with a description, reproducer, and
  the version / commit you tested against.
- We aim to acknowledge within five working days. Coordinated disclosure
  follows once a fix is available.

## Threat model

Cairn is a single static binary that serves geocoding queries from a
pre-built on-disk bundle. It does **not** call out to the network at
query time, does **not** execute user-supplied code, and does **not**
modify the bundle in place. The relevant attack surface is therefore:

| Surface | Risk |
|---------|------|
| HTTP request handling (axum, tower-http) | Header / query-string parsing |
| Tantivy text index | Index poisoning via crafted bundle |
| rkyv-archived spatial tiles | Deserialization soundness |
| Bundle integrity | Tampered tiles passing verification |
| Optional libpostal FFI | Unsafe C bindings (feature-gated) |
| Reverse-proxy auth | API key / `X-Forwarded-For` spoofing |

Mitigations:

- Bundle integrity is anchored by a per-tile blake3 hash recorded in
  `manifest.toml`. `cairn-build verify` recomputes every tile, every
  spatial blob, and every tantivy segment hash; mismatches fail loudly.
- The rkyv archive ref types use `check_archived_root` once at tile
  load and `archived_root` thereafter, sound only because the backing
  bytes are immutable mmap'd.
- API key auth is opt-in (`CAIRN_API_KEY=…`); rate limiting is opt-in
  (`CAIRN_RATE_LIMIT=…`) and only honors `X-Forwarded-For` when the
  per-connection peer is inside a configured CIDR allowlist
  (`CAIRN_TRUSTED_PROXIES=…`).
- The libpostal FFI is feature-gated and not built into the default
  image.

## Continuous CVE scanning

Every push to `main` and every pull request runs the Trivy scanner
against:

1. the production container image (`ghcr.io/cairn-geocoder/cairn:latest`)
2. the workspace source tree (`Cargo.lock`, `Dockerfile`)

A scheduled run also fires daily so that newly disclosed CVEs surface
on a schedule independent of repository activity.

Findings are uploaded as SARIF to GitHub Code Scanning. Public results
live at:

- **<https://github.com/cairn-geocoder/cairn/security>** — the
  Code Scanning tab on this repository.

The scan covers `CRITICAL`, `HIGH`, and `MEDIUM` severities; lower
severities are tracked by upstream dependency updaters but not blocked
on.

### Triage policy

| Severity | Action |
|----------|--------|
| CRITICAL | Fix or upstream-pin within 7 days; if no fix, ship a workaround. |
| HIGH | Fix in the next release; document in CHANGELOG. |
| MEDIUM | Fix as part of routine maintenance. |
| LOW / UNFIXED | Documented; not blocking. |

### Verifying yourself

Run Trivy locally on a release tag:

```sh
docker pull ghcr.io/cairn-geocoder/cairn:latest
trivy image --severity CRITICAL,HIGH ghcr.io/cairn-geocoder/cairn:latest
```

Or against a source checkout:

```sh
trivy fs --severity CRITICAL,HIGH .
```

## Supply chain

- Crate dependencies pass `cargo audit` (RustSec advisory DB) and
  `cargo deny` (license + bans + sources allowlist) on every CI run.
- The published image is rebuilt on every release tag from the source
  in this repository — no third-party base image other than
  `rust:alpine` (build) and `alpine` (runtime).
- Image attestations and SBOMs are queued as roadmap items
  (`cosign attest` + `syft`).

## Known issues / limitations

- **Open `MEDIUM`s in the runtime image** generally come from
  `rust:alpine`'s base layers; we follow the upstream Alpine release
  cadence for those.
- The optional libpostal FFI carries `unsafe extern "C"` calls. It is
  not enabled in the default build; only opt-in users with the
  `libpostal` cargo feature ship it.

For the live, version-pinned list of advisories affecting the current
release, see the GitHub Security tab linked above.
