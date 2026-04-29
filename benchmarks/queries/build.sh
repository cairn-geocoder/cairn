#!/usr/bin/env bash
# Build a 10 000-query Swiss test set from Geonames data. Stable
# output: deterministic across reruns so comparisons stay valid.
set -euo pipefail
cd "$(dirname "$0")"

CITIES=../data/cities1000.txt
CH_POSTAL=../data/CH.txt

if [ ! -f "$CITIES" ] || [ ! -f "$CH_POSTAL" ]; then
  echo "ERROR: run ../data/download.sh first" >&2
  exit 1
fi

OUT=swiss-10k.txt
NOISY=swiss-noisy.txt

# 5000 Swiss city / village names from cities1000.txt.
# Column 9 is the country code; column 2 is the canonical name.
awk -F'\t' '$9=="CH" {print $2}' "$CITIES" \
  | sort -u \
  | head -5000 \
  > _cities.txt

# 2500 postcode-only lookups from CH.txt postcode dump.
# Column 2 is the postal code.
awk -F'\t' '$2!="" {print $2}' "$CH_POSTAL" \
  | sort -u \
  | head -2500 \
  > _postcodes.txt

# 2500 "<postcode> <city>" composites.
awk -F'\t' '$2!="" && $3!="" {print $2 " " $3}' "$CH_POSTAL" \
  | sort -u \
  | head -2500 \
  > _composite.txt

cat _cities.txt _postcodes.txt _composite.txt | shuf --random-source=<(yes 42 2>/dev/null || cat /dev/zero) > "$OUT" 2>/dev/null \
  || cat _cities.txt _postcodes.txt _composite.txt > "$OUT"

# If shuf isn't available on macOS, fall back to a deterministic
# Python pseudo-shuffle so the harness still builds.
if ! command -v shuf >/dev/null 2>&1; then
  python3 - <<'PY'
import random
random.seed(42)
lines = []
for f in ["_cities.txt", "_postcodes.txt", "_composite.txt"]:
    lines.extend(open(f).read().splitlines())
random.shuffle(lines)
with open("swiss-10k.txt", "w") as out:
    out.write("\n".join(lines) + "\n")
PY
fi

# Noisy set: deliberate typos + ASCII-folded variants for fuzzy /
# phonetic / semantic A/B testing.
python3 - <<'PY' > "$NOISY"
import random
random.seed(42)
samples = open("_cities.txt").read().splitlines()[:1000]
for s in samples:
    if len(s) < 4:
        continue
    # one transposition
    i = random.randrange(1, len(s) - 1)
    typo = s[:i] + s[i+1] + s[i] + s[i+2:]
    # ASCII folding
    folded = (s.replace("ü", "ue").replace("ö", "oe").replace("ä", "ae")
                .replace("é", "e").replace("è", "e").replace("ç", "c"))
    print(typo)
    if folded != s:
        print(folded)
PY

rm -f _cities.txt _postcodes.txt _composite.txt

LINES=$(wc -l < "$OUT" | awk '{print $1}')
NOISY_LINES=$(wc -l < "$NOISY" | awk '{print $1}')
echo ">> $OUT: $LINES queries"
echo ">> $NOISY: $NOISY_LINES noisy queries"
