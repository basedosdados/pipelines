#!/usr/bin/env bash
# protect-raw.sh — PreToolUse hook for Data Basis onboarding sessions.
#
# Blocks Write and Edit tool calls targeting raw input directories.
# Raw data in input/ must never be modified — it is the immutable source of truth.
#
# Claude Code hook spec:
#   - Receives a JSON payload via stdin.
#   - Exit 0 → allow the tool call.
#   - Exit 2 + print JSON to stdout → block with a reason shown to the user.

set -euo pipefail

# Read the full payload from stdin
payload=$(cat)

tool_name=$(echo "$payload" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('tool_name',''))" 2>/dev/null || true)

# Only check Write and Edit tool calls
if [[ "$tool_name" != "Write" && "$tool_name" != "Edit" ]]; then
    exit 0
fi

# Extract the file path from the tool input
file_path=$(echo "$payload" | python3 -c "
import sys, json
d = json.load(sys.stdin)
inp = d.get('tool_input', {})
print(inp.get('file_path', inp.get('path', '')))
" 2>/dev/null || true)

if [[ -z "$file_path" ]]; then
    exit 0
fi

# Block writes to any path containing /input/ (raw data directory)
if echo "$file_path" | grep -qE '/input/|/input$'; then
    echo '{"decision":"block","reason":"Cannot modify raw input files. The input/ directory is immutable — it contains the original downloaded data. Write to output/ or pipelines/models/ instead."}'
    exit 2
fi

exit 0
