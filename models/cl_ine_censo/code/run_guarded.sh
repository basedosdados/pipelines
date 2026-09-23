#!/bin/bash
# Run clean.py under a hard memory guard.
#
# The first version of clean.py loaded whole tables into pandas and killed a
# 16 GB machine on `persona`. The rewrite streams in Arrow batches, but a guard
# costs nothing and means a regression stalls the job instead of the laptop.
#
# Usage: ./run_guarded.sh [--limit-gb N] [--script clean.py|upload.py] <arg> [<arg> ...]
set -uo pipefail

LIMIT_GB=6
SCRIPT=clean.py
while true; do
    case "${1:-}" in
        --limit-gb) LIMIT_GB="$2"; shift 2 ;;
        --script)   SCRIPT="$2";   shift 2 ;;
        *) break ;;
    esac
done
LIMIT_KB=$((LIMIT_GB * 1024 * 1024))
PYTHON="${PYTHON:-$HOME/.venvs/bd-pipelines/bin/python}"

"$PYTHON" "$SCRIPT" "$@" &
PID=$!

PEAK=0
while kill -0 "$PID" 2>/dev/null; do
    RSS=$(ps -o rss= -p "$PID" 2>/dev/null | tr -d ' ')
    if [[ -n "$RSS" ]]; then
        (( RSS > PEAK )) && PEAK=$RSS
        if (( RSS > LIMIT_KB )); then
            echo ""
            echo "ABORT: RSS $((RSS / 1024)) MB exceeded the ${LIMIT_GB} GB guard; killing $PID." >&2
            kill -9 "$PID" 2>/dev/null
            wait "$PID" 2>/dev/null
            echo "peak RSS: $((PEAK / 1024)) MB" >&2
            exit 99
        fi
    fi
    sleep 2
done

wait "$PID"
STATUS=$?
echo "peak RSS: $((PEAK / 1024)) MB (guard ${LIMIT_GB} GB)"
exit $STATUS
