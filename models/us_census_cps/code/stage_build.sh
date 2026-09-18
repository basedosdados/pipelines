#!/bin/bash
# Stage the CEPR Phase-A Stata build tree (step 4a).
#
# Builds OUTSIDE Dropbox (~/cps_build) because CEPR's programs gunzip/recompress in place and
# emit ~312 per-month .dta files — we keep that churn away from the pristine input/ and off Dropbox sync.
# Raw inputs are COPIED (not moved/linked) so models/us_census_cps/input/ stays untouched.
#
# NBER naming note: CEPR reads NBER-style inputs, and NBER's basic files are just the Census files
# renamed (CEPR's own comment: 'orginal file name "month"19pub.dat.Z'). So:
#   input/census_basic/<mmm><yy>pub.dat.gz  ->  $locbas/<year>/cpsb_<year>_<m>.txt.gz
# MORG: ORG calls `orgnber 1979 ... 1993`, so files must be 4-digit named (year var is already 4-digit,
# so CEPR's y2k conversion step is NOT needed):
#   input/nber_morg/morg<yy>.dta            ->  $locin/morg<yyyy>.dta
set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"
ROOT="${1:-$HOME/cps_build}"

DO="$ROOT/CPS_ORG/CEPR/DoFiles"
LOCBDO="$ROOT/CPS_Basic/CEPR/DoFiles"
LOCIN="$ROOT/CPS_ORG/NBER"
LOCBAS="$ROOT/CPS_Basic/NBER"
LOCTMP="$ROOT/CPS_ORG/CEPR/temp"
LOCOUT="$ROOT/CPS_ORG/CEPR"

mkdir -p "$DO" "$LOCBDO" "$LOCIN" "$LOCBAS" "$LOCTMP" "$LOCOUT" "$ROOT/logs"

echo "== 1. programs =="
cp "$SRC"/input/cepr_programs/org/*/*.do            "$DO"/
cp "$SRC"/input/cepr_programs/basic/*/*.do          "$LOCBDO"/
cp "$SRC"/input/cepr_programs/basic/*/*.dct         "$LOCBDO"/
cp "$SRC"/input/cepr_programs/march/*/*.do          "$DO"/ 2>/dev/null || true
cp "$SRC"/input/cepr_programs/march/*/*.dct         "$LOCBDO"/ 2>/dev/null || true
echo "   org/march do -> $DO ($(ls "$DO"/*.do | wc -l | tr -d ' ')), basic do+dct -> $LOCBDO ($(ls "$LOCBDO" | wc -l | tr -d ' '))"

echo "== 2. MORG 1979-1993 (2-digit -> 4-digit names) =="
for yy in $(seq -w 79 93); do
  s="$SRC/input/nber_morg/morg${yy}.dta"
  [ -f "$s" ] || { echo "   MISSING $s"; exit 1; }
  cp "$s" "$LOCIN/morg19${yy}.dta"
done
echo "   staged $(ls "$LOCIN"/morg*.dta | wc -l | tr -d ' ') MORG files"

echo "== 3. basic monthly 1994-2019 (Census -> NBER naming) =="
MONTHS=(jan feb mar apr may jun jul aug sep oct nov dec)
n=0; missing=0
for y in $(seq 1994 2019); do
  yy=$(printf "%02d" $((y % 100)))
  mkdir -p "$LOCBAS/$y"
  for i in $(seq 1 12); do
    m=${MONTHS[$((i-1))]}
    s="$SRC/input/census_basic/${m}${yy}pub.dat.gz"
    d="$LOCBAS/$y/cpsb_${y}_${i}.txt.gz"
    if [ -f "$s" ]; then [ -f "$d" ] || cp "$s" "$d"; n=$((n+1)); else echo "   missing raw: ${m}${yy}"; missing=$((missing+1)); fi
  done
done
echo "   staged $n monthly files ($missing missing)"

echo "== 4. patch master for macOS/local paths =="
M="$DO/cepr_org_master.do"
sed -i '' \
  -e 's|^global gnulin = 0|global gnulin = 1|' \
  -e "s|\"/CPS_ORG/CEPR/Do\"|\"$DO\"|" \
  -e "s|\"/CPS_Basic/CEPR/DoFiles\"|\"$LOCBDO\"|" \
  -e "s|\"/CPS_ORG/NBER\"|\"$LOCIN\"|" \
  -e "s|\"/CPS_Basic/NBER\"|\"$LOCBAS\"|" \
  -e "s|\"/CPS_ORG/CEPR/temp\"|\"$LOCTMP\"|" \
  -e "s|\"/CPS_ORG/CEPR/\"|\"$LOCOUT\"|" \
  "$M"
echo "   patched globals:"
grep -nE '^global (gnulin|do|locbdo|locin|locbas|loctmp|locout) ' "$M" | sed 's/^/     /'
echo
echo "STAGED at $ROOT"
