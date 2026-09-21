#!/usr/bin/env bash
# Supervisor for the ComprasNet backfill.
#
# Runs on a DEDICATED interpreter, never `uv run`. The repo shares one venv
# (UV_PROJECT_ENVIRONMENT=~/.venvs/bd-pipelines), and any `uv run` anywhere in
# the repo — a lint, a type check, or merely opening a new Claude session in
# this worktree — re-syncs it. On 2026-09-21 that removed certifi's cacert.pem
# out from under the running harvest, which died with
# "Could not find a suitable TLS CA certificate bundle". A multi-day process
# must not share a mutable environment with interactive work.
#
#   uv venv --python 3.11 ~/.venvs/comprasnet-harvest
#   VIRTUAL_ENV=~/.venvs/comprasnet-harvest uv pip install requests pyarrow
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
PYTHON="${HARVEST_PYTHON:-$HOME/.venvs/comprasnet-harvest/bin/python}"

mkdir -p "$DATA"
cd "$REPO" || exit 1

# Single-instance lock. Two supervisors racing the same chunk files duplicate
# every request and interleave their writes; mkdir is atomic, so it is a
# reliable lock even without flock (which macOS does not ship).
LOCK="$DATA/.supervisor.lock"
release_lock() { rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null; }
if ! mkdir "$LOCK" 2>/dev/null; then
    holder=$(cat "$LOCK/pid" 2>/dev/null || echo 0)
    if kill -0 "$holder" 2>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR already running as pid $holder, exiting" >> "$LOG"
        exit 0
    fi
    echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR clearing stale lock from pid $holder" >> "$LOG"
fi
echo $$ > "$LOCK/pid"
trap release_lock EXIT INT TERM

fast_failures=0
for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
    run_started=$SECONDS
    echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR starting attempt $attempt" >> "$LOG"
    "$PYTHON" models/br_mgi_compras_publicas/code/harvest_comprasnet.py \
        --start "$START" --end "$END" --workers 8 >> "$LOG" 2>&1
    code=$?
    SECONDS_THIS_RUN=$((SECONDS - run_started))
    if [ "$code" -eq 0 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR harvest completed" >> "$LOG"
        exit 0
    fi
    # A run that dies almost immediately is an environment fault, not a flaky
    # connection; retrying it 200 times in an hour just hides the cause.
    if [ "$SECONDS_THIS_RUN" -lt 60 ]; then
        fast_failures=$((fast_failures + 1))
    else
        fast_failures=0
    fi
    if [ "$fast_failures" -ge 5 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR 5 immediate failures, stopping" >> "$LOG"
        exit 1
    fi
    echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR exit=$code, retrying in 60s" >> "$LOG"
    sleep 60
done

echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR gave up after $MAX_ATTEMPTS attempts" >> "$LOG"
exit 1
