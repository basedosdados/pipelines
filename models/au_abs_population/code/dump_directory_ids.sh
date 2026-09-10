#!/usr/bin/env bash
# Dump the br_bd_diretorios_au identifier lists that validate.py checks FKs against.
# Usage: bash dump_directory_ids.sh [out_dir]   (default /tmp)
set -euo pipefail
OUT="${1:-/tmp}"
for t in sa2_2021 sa3_2021 sa4_2021 gccsa_2021 lga_2021 state; do
  col="id_$(echo "$t" | sed 's/_20[0-9][0-9]//')"
  bq query --project_id=basedosdados-dev --use_legacy_sql=false --format=csv --max_rows=100000 \
    "select $col from \`basedosdados.br_bd_diretorios_au.$t\`" | tail -n +2 > "$OUT/dir_$t.txt"
  echo "$t: $(wc -l < "$OUT/dir_$t.txt") ids"
done
