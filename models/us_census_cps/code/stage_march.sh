#!/bin/bash
# Stage the CEPR March/ASEC Phase-A build (step 4a, table `march`, coverage 2014-2018 per D-coverage-cap).
#
# Same NBER/CEPR naming trick as basic: CEPR's own comments give the mapping, e.g.
#   m2015: !gzip -d cpsm_2015.txt.gz   /*orginal file name asec2015_pubuse.dat.gz*/
#   m2014: cpsm_2014.txt.gz            /*orginal file name asec2014_pubuse_tax_fix_5x8.dat.gz*/
# Files stay GZIPPED here - the m<year> programs gunzip and recompress themselves.
#
# The master also processes 1980-2013 from UNICON (proprietary, we don't have it) -> those
# invocations are commented out, leaving only the Census path (2014-2018).
set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"
ROOT="${1:-$HOME/cps_build}"

MDO="$ROOT/CPS_March/CEPR/Do"
LOCIN="$ROOT/CPS_March/Raw/Unicon"      # pre-2014 UNICON - unused, kept empty
LOCRAW="$ROOT/CPS_March/Raw/Census"
LOCTMP="$ROOT/CPS_March/CEPR/Working"
LOCINXT="$ROOT/CPS_March/Census9704"    # 1997-2004 HI appendages - unused
REPWGT="$ROOT/CPS_March/Raw/rep_wgt_data"  # unused (master's repwgt call is commented out)
LOCOUT="$ROOT/CPS_March/CEPR"

mkdir -p "$MDO" "$LOCIN" "$LOCRAW" "$LOCTMP" "$LOCINXT" "$REPWGT" "$LOCOUT"

echo "== 1. march programs =="
cp "$SRC"/input/cepr_programs/march/*/*.do  "$MDO"/
cp "$SRC"/input/cepr_programs/march/*/*.dct "$MDO"/
echo "   $(ls "$MDO"/*.do | wc -l | tr -d ' ') .do, $(ls "$MDO"/*.dct | wc -l | tr -d ' ') .dct -> $MDO"

echo "== 2. raw Census March -> CEPR naming (kept gzipped) =="
stage() { # srcfile destdir destname
  mkdir -p "$2"
  if [ -f "$SRC/input/census_march/$1" ]; then
    [ -f "$2/$3" ] || cp "$SRC/input/census_march/$1" "$2/$3"
    echo "   $1 -> $(basename "$2")/$3"
  else
    echo "   MISSING SOURCE: $1"; return 1
  fi
}
stage asec2014_pubuse_tax_fix_5x8_2017.dat.gz "$LOCRAW/2014" cpsm_2014.txt.gz
stage asec2014_pubuse_3x8_rerun_v2.dat.gz     "$LOCRAW/2014" cpsm_2014_redes.txt.gz
stage asec2015_pubuse.dat.gz                  "$LOCRAW/2015" cpsm_2015.txt.gz
stage asec2016_pubuse_v3.dat.gz               "$LOCRAW/2016" cpsm_2016.txt.gz
stage asec2017_pubuse.dat.gz                  "$LOCRAW/2017" cpsm_2017.txt.gz
stage asec2018_pubuse.dat.gz                  "$LOCRAW/2018" cpsm_2018.txt.gz

echo "== 3. patch march master (paths + disable UNICON path) =="
M="$MDO/cepr_march_master.do"
sed -i '' \
  -e 's|^global gnulin = 0|global gnulin = 1|' \
  -e "s|\"/ceprdata/CPS_March/CEPR/Do/\"|\"$MDO\"|" \
  -e "s|\"/ceprdata/CPS_March/Raw/Unicon/\"|\"$LOCIN\"|" \
  -e "s|\"/ceprdata/CPS_March/Raw/Census/\"|\"$LOCRAW\"|" \
  -e "s|\"/ceprdata/CPS_March/CEPR/Working/\"|\"$LOCTMP\"|" \
  -e "s|\"/ceprdata/CPS_March/Census9704\"|\"$LOCINXT\"|" \
  -e "s|\"/ceprdata/CPS_March/Raw/rep_wgt_data/\"|\"$REPWGT\"|" \
  -e "s|\"/ceprdata/CPS_March/CEPR/\"|\"$LOCOUT\"|" \
  "$M"

# Disable UNICON-dependent steps (no Unicon data; D-coverage-cap keeps march at 2014-2018).
# y2k (line ~106) converts UNICON 2-digit years; marunicon1/2/3 process 1980-2013 + 2001s.
awk '
  /^do "cepr_march_y2k\.do"/                  { print "* [SKIP: UNICON not available] " $0; next }
  /^marunicon1 / || /^\*\/ 1993 1994 1995/    { print "* [SKIP: UNICON not available] " $0; next }
  /^marunicon2 / || /^marunicon3 /            { print "* [SKIP: UNICON not available] " $0; next }
  { print }
' "$M" > "$M.tmp" && mv "$M.tmp" "$M"

echo "   globals:"; grep -nE '^global (gnulin|do|locin|locraw|loctmp|locinxt|repwgt|locout) ' "$M" | head -8 | sed 's/^/     /'
echo "   disabled:"; grep -n '\[SKIP: UNICON' "$M" | sed 's/^/     /'
echo "   kept:"; grep -nE '^(m201[4-8]|marcensus2? )' "$M" | sed 's/^/     /'
echo
echo "STAGED march at $ROOT/CPS_March"
