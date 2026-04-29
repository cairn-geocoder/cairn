#!/usr/bin/env bash
# Stand up Pelias on Switzerland. First run is slow (~30-60 min)
# because the OSM importer indexes ~480 MB of PBF into Elasticsearch.
# Subsequent runs reuse `data/pelias/elasticsearch` and start in
# ~30 s.
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p data/pelias

# Hand-rolled pelias.json — country=CH, source=osm only (we explicitly
# isolate to OSM for fairness vs Cairn / Nominatim / Photon).
cat > data/pelias/pelias.json <<'JSON'
{
  "logger": { "level": "info", "timestamp": true },
  "esclient": {
    "hosts": [{ "host": "elasticsearch", "port": 9200 }]
  },
  "elasticsearch": {
    "settings": {
      "index": { "number_of_replicas": 0, "number_of_shards": 1 }
    }
  },
  "api": {
    "services": {
      "libpostal": { "url": "http://libpostal:4400" }
    }
  },
  "imports": {
    "openstreetmap": {
      "leveldbpath": "/tmp",
      "datapath": "/data/openstreetmap",
      "import": [{ "filename": "switzerland-latest.osm.pbf" }]
    }
  }
}
JSON

# Stage the PBF where the OSM importer expects it.
mkdir -p data/pelias/openstreetmap
cp -n data/switzerland-latest.osm.pbf data/pelias/openstreetmap/

export PELIAS_DATA=$(pwd)/data/pelias

echo "==> elasticsearch up"
docker compose -f compose/docker-compose.pelias.yml up -d elasticsearch libpostal

# Wait for ES.
for _ in $(seq 1 60); do
  if curl -sf http://127.0.0.1:9200/_cluster/health >/dev/null; then break; fi
  sleep 2
done

echo "==> creating Pelias schema"
docker compose -f compose/docker-compose.pelias.yml run --rm schema || true

START=$(date +%s)
echo "==> importing OSM (this takes a while)"
docker compose -f compose/docker-compose.pelias.yml run --rm openstreetmap
END=$(date +%s)
IMPORT_SEC=$((END - START))

echo "==> bringing up API"
docker compose -f compose/docker-compose.pelias.yml up -d api

# Wait for API ready.
for _ in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:4000/v1/search?text=Vaduz&size=1" >/dev/null; then
    break
  fi
  sleep 2
done

DISK=$(du -sk data/pelias | awk '{print $1 * 1024}')
mkdir -p results
cat > results/pelias-import.json <<EOF
{
  "tool": "pelias",
  "import_seconds": $IMPORT_SEC,
  "disk_bytes": $DISK,
  "components": ["elasticsearch", "libpostal", "openstreetmap-importer", "api"]
}
EOF
echo "==> Pelias ready on http://127.0.0.1:4000"
cat results/pelias-import.json
