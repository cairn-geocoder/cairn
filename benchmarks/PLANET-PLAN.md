# Planet-scale benchmark plan

## Why

A4.1 + A4.4 demonstrated Cairn at country scale (CH 506 MB, DE
4.7 GB). Open question for serious operators: **does the curve hold
at continent / planet scale?** Pelias and Nominatim are the only
incumbents that reliably claim planet-scale today; matching that
unblocks "drop-in replacement" positioning.

## Observed scaling baseline

| Input | PBF | Build wall | Peak build RSS | Bundle disk | Hot serve RSS | p99 |
|---|---:|---:|---:|---:|---:|---:|
| Switzerland | 506 MB | 27 s | 8.3 GB | 212 MB | 102 MB | 1.32 ms |
| Germany     | 4.7 GB | 633 s | 22 GB | 1.67 GB | 392 MB | 3.71 ms |
| Ratio (DE/CH) | 9.3× | 23× | 2.7× | 7.9× | 3.8× | 2.8× |

Build wall-clock superlinear (PBF size → admin polygon assembly
walks more relations). RSS sublinear. Bundle disk ~linear. Serve
hot RSS sublinear because rkyv tiles stay mmap'd; resident set is
what's touched.

## Headroom on this host

- 36 GB physical RAM, ~30 GB usable after OS / OrbStack overhead.
- DE peak build RSS already burned 22 GB.
- Linear extrapolation to Europe (PBF 34 GB ≈ 7.3× DE):
  **projected peak build RSS ~160 GB** — won't fit.
- Conservative-sublinear extrapolation (cube-root of 7.3): still
  ~40 GB peak. **Borderline at best on 36 GB.**

⇒ Mac harness is a **dead end for planet** without optimizing the
build to stream tiles to disk instead of accumulating in memory.
Country-grid option below is the only thing that fits on this box.

## Three viable paths

### Option 1 — Multi-country grid (free, fits the Mac)

Run `./cairn/build.sh <country>` against 4-6 mid-size countries
that individually fit 36 GB:

- France (4.5 GB PBF, ~5 M places)
- United Kingdom (1.9 GB PBF, ~3 M places)
- Italy (2.2 GB PBF, ~2.5 M places)
- Spain (1.8 GB PBF, ~2 M places)
- Netherlands (1.4 GB PBF, ~3 M places)
- Brazil (3.3 GB PBF, ~3 M places)

Yields 6-point scaling curve from CH (520 k) → DE (3 M) →
country grid → multi-country aggregate. Validates "Cairn scales
predictably" without requiring planet-scale RAM. Total wall-clock
~30-60 minutes on this host. **Cost: $0.**

`./cairn/build.sh france && ./cairn/build.sh united-kingdom && …`
already supported by the country-aware harness shipped in
phase A4.1.

### Option 2 — Continent (Europe) on a rented cloud box

Geofabrik europe-latest.osm.pbf = 34.3 GB.
Estimated peak build RSS: 80-160 GB. Estimated wall-clock:
1.5-3 hours on Apple-class single-thread.

Cloud target: **AWS c6i.16xlarge** (64 vCPU / 128 GB RAM, x86_64,
~$2.76/hr on-demand, ~$0.83/hr spot). Build + bench fits in one
~3-hour window: **<$10 spot, <$30 on-demand**.

Workflow stub already exists at `.github/workflows/bench.yml`
(country-aware, ubuntu-latest x86_64, supports `workflow_dispatch`
with country slug). Add `europe` to the country case statement and
override the runner to a self-hosted Hetzner / EC2 box with
≥128 GB RAM. Spot c6i / Hetzner CCX63 (32 vCPU / 128 GB) at
€0.95/hr from `hcloud` is the cheapest path.

### Option 3 — Planet on a beefy cloud box

planet-latest.osm.pbf ≈ 80-90 GB compressed, ~3 B nodes / 350 M
ways / 4.5 M relations, ~60-80 M Place candidates.

Estimated peak build RSS: 200-400 GB. Estimated wall-clock:
8-24 hours on a single high-memory box.

Cloud target: **AWS r7iz.16xlarge** (64 vCPU / 512 GB RAM,
~$10.99/hr on-demand) or **r7iz.32xlarge** (128 vCPU / 1024 GB,
~$22/hr). One full build at on-demand pricing: **~$200-500**.
Spot pricing typically 30-40 % of on-demand.

Alternative: Hetzner dedicated AX102 (256 GB RAM, ~€300/month)
amortizes if planet is rebuilt monthly.

⚠ This run will surface real bottlenecks — current `cairn-build`
loads the full PBF node coord cache into RAM (Pass 1) before
emitting Place stream. At 3 B nodes that's ~36 GB just for the
cache (12 bytes / node). May need to switch to a disk-spilling
node cache (osmium-style flatnode file) before planet works at
all, regardless of host RAM.

## Recommendation

1. **This week (free, this host):** run the multi-country grid
   (option 1). 6 countries × ~5 min build avg = 30 min total. Adds
   6 data points to the scaling curve, validates "still sublinear
   hot RSS" claim without spending a dollar. Publish results into
   `REPORT.md` as "Country grid" section.

2. **Within 2 weeks (rent box, ~$10):** rent a CCX63 spot
   instance, run option 2 (Europe). Confirms continental scale or
   surfaces the flatnode-cache blocker before investing in planet.

3. **After option 2 lands cleanly:** plan option 3 (planet) only
   if option 2 proved out the build path. Otherwise scope a
   `cairn-build --flatnode <path>` work item first to get RAM out
   of the critical path.

## What to ship in the harness ahead of time

- [ ] Add `europe`, `france`, `united-kingdom`, `italy`, `spain`,
      `netherlands`, `brazil` to `data/download.sh`'s country case
      (most already work via the 2-char fallback, but ISO2 mapping
      needs verification — UK → GB, not UK).
- [ ] Add `world` slug to `bench.yml` workflow with a separate
      `runs-on` selector for self-hosted >128 GB box.
- [ ] Document the flatnode-cache item as a Phase 6f roadmap entry
      so the planet-scale blocker is tracked.

## Estimated headline numbers (if option 2 succeeds)

Conservative interpolation between DE actuals and the projected
sublinear curve:

| Engine | Build wall | Hot RSS | p99 | RPS peak |
|---|---:|---:|---:|---:|
| Cairn (Europe) | ~30-90 min | ~2-4 GB | 5-10 ms | 3-5 k |
| Pelias (Europe) | 4-8 h | 25-40 GB | 100-200 ms | ~150 |
| Photon (Europe) | 1-2 h | 8-15 GB | 200-500 ms | ~800 |
| Nominatim (Europe) | 2-5 days | 15-25 GB | 50-100 ms | ~500 |

These are projections, not measurements. Replace with real numbers
once option 2 runs.
