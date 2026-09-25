#!/usr/bin/env bash
# Supervise a long TCE-MG harvest: keep harvest_mg.py alive, keep the token
# bridge alive, and survive the things that kill a 20-hour job.
#
# The harvester already handles the expected failures itself -- it parks on an
# expired token and retries a dropped transfer. This wrapper exists for the
# UNEXPECTED ones: an OOM kill, a laptop sleep that breaks every socket at once,
# an unhandled exception in a code path nobody hit in testing. Every one of those
# is safe to answer by starting again, because the manifest and the ledger make a
# restart resume rather than redo -- so the only real failure mode left is the
# process being gone and nobody noticing for six hours.
#
# Deliberately NOT systemd/launchd: this is a one-off backfill that a human is
# watching, not a service. `pkill -f harvest_mg.py` stops it.
#
# Usage:  bash run_mg_harvest.sh            # supervise
#         bash run_mg_harvest.sh status     # one-shot progress report
set -uo pipefail

DATA="${MG_DATA_DIR:-$HOME/Downloads/world_wb_mides_data}"
TOKEN="${MG_TOKEN_FILE:-$DATA/.mg_token}"
VENV="${MG_VENV:-$HOME/.venvs/mg-harvest}"
PORT="${MG_BRIDGE_PORT:-8787}"
LOG="$DATA/harvest_mg.log"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"

status() {
    local mg="$DATA/input/mg"
    echo "=== MG harvest status $(date '+%Y-%m-%d %H:%M:%S') ==="
    if [ -f "$TOKEN" ]; then
        "$VENV/bin/python" - "$TOKEN" <<'PY'
import base64, json, sys, datetime
try:
    t = open(sys.argv[1]).read().strip()
    p = t.split(".")[1]; p += "=" * (-len(p) % 4)
    exp = json.loads(base64.urlsafe_b64decode(p))["exp"]
    left = (exp - datetime.datetime.now(datetime.timezone.utc).timestamp()) / 60
    print(f"token      : {left:.0f} min left" if left > 0 else
          f"token      : EXPIRED {abs(left):.0f} min ago -- harvester is parked")
except Exception as exc:
    print(f"token      : unreadable ({exc})")
PY
    else
        echo "token      : MISSING at $TOKEN"
    fi
    pgrep -f bulk_mg.py       >/dev/null && echo "bulk       : running" || echo "bulk       : not running"
    pgrep -f harvest_mg.py    >/dev/null && echo "fallback   : running" || echo "fallback   : not running"
    pgrep -f mg_token_bridge  >/dev/null && echo "bridge     : running on $PORT" || echo "bridge     : NOT running"
    if [ -f "$mg/_ledger.jsonl" ]; then
        local ok fail zips parts
        # `grep -c` prints 0 AND exits 1 when there are no matches, so a
        # `|| echo 0` fallback prints the count twice. Let it print its own 0.
        ok=$(grep -c '"status": "ok"' "$mg/_ledger.jsonl" 2>/dev/null); ok=${ok:-0}
        fail=$(grep -c '"status": "failed"' "$mg/_ledger.jsonl" 2>/dev/null); fail=${fail:-0}
        zips=$(find "$mg" -name '*.zip' 2>/dev/null | wc -l | tr -d ' ')
        parts=$(find "$mg" -name '*.part' 2>/dev/null | wc -l | tr -d ' ')
        echo "files ok   : $ok   failed: $fail   on disk: $zips   in flight: $parts"
        echo "size       : $(du -sh "$mg" 2>/dev/null | cut -f1)"
    else
        echo "ledger     : none yet"
    fi
    if [ -f "$mg/_bulk_ledger.jsonl" ]; then
        echo "bulk pairs : $(grep -c '"status": "ok"' "$mg/_bulk_ledger.jsonl" 2>/dev/null) of 52 complete"
    fi
    # Two logs exist: the supervisor's own, and bulk_mg.log when bulk was
    # launched by hand. Show whichever was written most recently, so status never
    # reports a stale phase.
    local newest
    newest=$(ls -t "$LOG" "$DATA/bulk_mg.log" 2>/dev/null | head -1)
    echo "--- last 6 lines of $(basename "${newest:-$LOG}") ---"
    tail -6 "${newest:-$LOG}" 2>/dev/null
}

if [ "${1:-}" = "status" ]; then status; exit 0; fi

mkdir -p "$DATA"
# The bridge is how a human gets a refreshed token onto disk without pasting 5 KB
# through a terminal. Harmless to start twice; the bind just fails.
if ! pgrep -f mg_token_bridge >/dev/null; then
    nohup "$VENV/bin/python" "$HERE/mg_token_bridge.py" \
        --token-file "$TOKEN" --port "$PORT" >> "$DATA/bridge.log" 2>&1 &
    echo "started token bridge on $PORT"
fi

# Two phases, in order.
#
#   1. bulk_mg.py, ONCE. It fetches only the exercise/category packages under its
#      ~45 MB ceiling -- five of fifty-two -- because the gateway truncates
#      bigger responses at ~55 MB no matter how often they are retried. Cheap,
#      and it removes ~6,800 requests from phase 2.
#   2. harvest_mg.py, in a restart loop, for everything else: 26 GB one
#      municipality at a time. It reads the bulk ledger and skips what phase 1
#      already covered.
#
# Phase 2 is the one that is restarted, because it is the one that runs for
# hours. Its manifest and ledger make every restart resume rather than redo, so a
# crash, an OOM or a laptop sleep costs one file, not a night.
if [ ! -f "$DATA/.bulk_phase_done" ]; then
    echo "[$(date '+%F %T')] bulk phase (small packages only)" | tee -a "$LOG"
    ( cd "$REPO" && MG_TOKEN_FILE="$TOKEN" "$VENV/bin/python" -u \
        "$HERE/bulk_mg.py" --workers "${MG_BULK_WORKERS:-4}" >> "$LOG" 2>&1 )
    touch "$DATA/.bulk_phase_done"
    echo "[$(date '+%F %T')] bulk phase finished" | tee -a "$LOG"
fi

attempt=0
while true; do
    if pgrep -f harvest_mg.py >/dev/null; then sleep 30; continue; fi
    attempt=$((attempt + 1))
    echo "[$(date '+%F %T')] starting per-municipality harvest (attempt $attempt)" | tee -a "$LOG"
    ( cd "$REPO" && MG_TOKEN_FILE="$TOKEN" "$VENV/bin/python" -u \
        "$HERE/harvest_mg.py" --workers "${MG_WORKERS:-12}" >> "$LOG" 2>&1 )
    code=$?
    if [ $code -eq 0 ]; then
        echo "[$(date '+%F %T')] harvest complete" | tee -a "$LOG"
        status
        exit 0
    fi
    echo "[$(date '+%F %T')] exited $code -- restarting in 60s" | tee -a "$LOG"
    sleep 60
done
