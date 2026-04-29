#!/usr/bin/env bash
# Build a 10 000-query test set from Geonames data. Stable + country-
# aware. First arg = country slug (default switzerland) → ISO2 code:
#   switzerland|liechtenstein|germany|france|... see lookup below.
set -euo pipefail
cd "$(dirname "$0")"

COUNTRY="${1:-switzerland}"
case "$COUNTRY" in
  switzerland)   CC=CH ;;
  liechtenstein) CC=LI ;;
  germany)       CC=DE ;;
  france)        CC=FR ;;
  italy)         CC=IT ;;
  austria)       CC=AT ;;
  *)             CC=$(printf '%s' "$COUNTRY" | head -c2 | tr 'a-z' 'A-Z') ;;
esac

CITIES=../data/cities1000.txt
COUNTRY_POSTAL=../data/${CC}.txt

if [ ! -f "$CITIES" ]; then
  echo "ERROR: ../data/download.sh first (cities1000.txt missing)" >&2
  exit 1
fi

OUT=${COUNTRY}-10k.txt
NOISY=${COUNTRY}-noisy.txt

awk -F'\t' -v cc="$CC" '$9==cc {print $2}' "$CITIES" \
  | sort -u | head -5000 > _cities.txt

if [ -f "$COUNTRY_POSTAL" ]; then
  awk -F'\t' '$2!="" {print $2}' "$COUNTRY_POSTAL" \
    | sort -u | head -2500 > _postcodes.txt
  awk -F'\t' '$2!="" && $3!="" {print $2 " " $3}' "$COUNTRY_POSTAL" \
    | sort -u | head -2500 > _composite.txt
else
  echo "WARN: $COUNTRY_POSTAL missing — running cities-only" >&2
  : > _postcodes.txt
  : > _composite.txt
fi

if command -v shuf >/dev/null 2>&1; then
  cat _cities.txt _postcodes.txt _composite.txt \
    | shuf --random-source=<(yes 42 2>/dev/null || cat /dev/zero) > "$OUT" 2>/dev/null \
    || cat _cities.txt _postcodes.txt _composite.txt > "$OUT"
else
  python3 - <<PY
import random
random.seed(42)
lines = []
for f in ["_cities.txt", "_postcodes.txt", "_composite.txt"]:
    lines.extend(open(f).read().splitlines())
random.shuffle(lines)
with open("${OUT}", "w") as out:
    out.write("\n".join(lines) + "\n")
PY
fi

# Noisy set deterministic.
python3 - <<PY > "$NOISY"
import random
random.seed(42)
samples = open("_cities.txt").read().splitlines()[:1000]
for s in samples:
    if len(s) < 4: continue
    i = random.randrange(1, len(s) - 1)
    typo = s[:i] + s[i+1] + s[i] + s[i+2:]
    folded = (s.replace("ü","ue").replace("ö","oe").replace("ä","ae")
                .replace("é","e").replace("è","e").replace("ç","c"))
    print(typo)
    if folded != s:
        print(folded)
PY

# Backwards-compat: switzerland still gets the legacy filename so the
# existing run.sh + run-rps.sh keep working without changes.
if [ "$COUNTRY" = "switzerland" ]; then
  cp "$OUT" swiss-10k.txt
  cp "$NOISY" swiss-noisy.txt
fi

rm -f _cities.txt _postcodes.txt _composite.txt

echo ">> $OUT: $(wc -l < "$OUT") queries"
echo ">> $NOISY: $(wc -l < "$NOISY") noisy queries"
