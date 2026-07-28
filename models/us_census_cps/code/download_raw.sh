#!/bin/bash
# Download raw inputs for the CEPR CPS reproduction (step 3).
# Resumable + idempotent: skips files already present; failed/absent URLs are skipped, not fatal.
# Usage: bash download_raw.sh [basic|march|morg|all]   (default: all)
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
IN="$HERE/input"
BASIC="$IN/census_basic"; MARCH="$IN/census_march"; MORG="$IN/nber_morg"
mkdir -p "$BASIC" "$MARCH" "$MORG"
MONTHS="jan feb mar apr may jun jul aug sep oct nov dec"
WHAT="${1:-all}"

get() { # url dest
  [ -s "$2" ] && { echo "have  $(basename "$2")"; return 0; }
  if curl -sfL -A "Mozilla/5.0" -o "$2" "$1"; then echo "ok    $(basename "$2")"; else rm -f "$2"; echo "miss  $(basename "$2")  ($1)"; return 1; fi
}

if [ "$WHAT" = all ] || [ "$WHAT" = basic ]; then
  echo "== Basic Monthly (1994-2025) =="
  for y in $(seq 1994 2026); do yy=$(printf "%02d" $((y % 100)))
    for m in $MONTHS; do get "https://www2.census.gov/programs-surveys/cps/datasets/${y}/basic/${m}${yy}pub.dat.gz" "$BASIC/${m}${yy}pub.dat.gz"; done
  done
fi

if [ "$WHAT" = all ] || [ "$WHAT" = march ]; then
  echo "== March / ASEC (2014-2025) =="
  for y in $(seq 2014 2026); do yy=$(printf "%02d" $((y % 100)))
    # CSV pack exists ~2019+; fixed-width pubuse (some years _v3) covers 2014-2018
    get "https://www2.census.gov/programs-surveys/cps/datasets/${y}/march/asecpub${yy}csv.zip" "$MARCH/asecpub${yy}csv.zip" \
      || get "https://www2.census.gov/programs-surveys/cps/datasets/${y}/march/asec${y}_pubuse.dat.gz" "$MARCH/asec${y}_pubuse.dat.gz" \
      || get "https://www2.census.gov/programs-surveys/cps/datasets/${y}/march/asec${y}_pubuse_v3.dat.gz" "$MARCH/asec${y}_pubuse_v3.dat.gz"
  done
fi

if [ "$WHAT" = all ] || [ "$WHAT" = morg ]; then
  echo "== NBER MORG (1979-1993, ORG history) =="
  for y in $(seq 1979 1993); do yy=$(printf "%02d" $((y % 100)))
    get "https://data.nber.org/morg/annual/morg${yy}.dta" "$MORG/morg${yy}.dta"
  done
fi
echo "DONE ($WHAT)"
