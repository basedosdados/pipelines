#!/usr/bin/env bash
# Supervisor for the ComprasNet backfill.
#
# Every phase is resumable at month granularity, so restarting after a crash
# costs at most the month in flight. Without this, a single dropped connection
# ends the run and it stays dead until somebody looks — which is exactly what
# happened on 2026-09-18, losing two and a half days.
#
#   bash models/br_mgi_compras_publicas/code/run_comprasnet_backfill.sh
set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DATA="${COMPRAS_DATA_DIR:-$HOME/Downloads/br_mgi_compras_publicas_data}"
LOG="$DATA/comprasnet_backfill.log"
START="${1:-2001-06}"
END="${2:-2024-01}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-200}"

mkdir -p "$DATA"
cd "$REPO" || exit 1

for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
    echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR starting attempt $attempt" >> "$LOG"
    uv run python models/br_mgi_compras_publicas/code/harvest_comprasnet.py \
        --start "$START" --end "$END" --workers 8 >> "$LOG" 2>&1
    code=$?
    if [ "$code" -eq 0 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR harvest completed" >> "$LOG"
        exit 0
    fi
    echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR exit=$code, retrying in 60s" >> "$LOG"
    sleep 60
done

echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR gave up after $MAX_ATTEMPTS attempts" >> "$LOG"
exit 1
