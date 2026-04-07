#!/usr/bin/env python3
"""
Pre-compact hook for Data Basis onboarding sessions.

Fires before Claude Code compresses the conversation context.
Saves the current onboarding state to a dataset-scoped file so the
orchestrator can resume from the correct step after context compression.

Claude Code hook spec:
- Receives a JSON payload via stdin with conversation metadata.
- Exits 0 to allow compaction to proceed.
- Writes state to pipelines/models/<dataset_slug>/onboarding-state.json
  so the file survives terminal restarts (unlike /tmp/).
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path


def extract_state_from_transcript(transcript: str) -> dict:
    """
    Parse the conversation transcript to extract onboarding state.
    Looks for the structured context block and step completion markers.
    """
    state = {
        "saved_at": datetime.now().isoformat(),
        "dataset_slug": None,
        "current_step": None,
        "completed_steps": [],
        "context_block": None,
        "discovered_ids_block": None,
        "architecture_urls": {},
        "drive_folder": None,
    }

    # Extract dataset slug from context block
    slug_match = re.search(r"=== DATASET CONTEXT: (\S+) ===", transcript)
    if slug_match:
        state["dataset_slug"] = slug_match.group(1)

    # Extract the full context block
    ctx_match = re.search(
        r"(=== DATASET CONTEXT:.*?===)", transcript, re.DOTALL
    )
    if ctx_match:
        state["context_block"] = ctx_match.group(1)

    # Extract discovered IDs block
    ids_match = re.search(
        r"(=== DISCOVERED IDs.*?(?===|$))", transcript, re.DOTALL
    )
    if ids_match:
        state["discovered_ids_block"] = ids_match.group(1)

    # Detect completed steps from step markers
    step_patterns = {
        1: r"Step 1 complete|context.*complete|=== DATASET CONTEXT:",
        2: r"Step 2 complete|architecture.*complete|=== ARCHITECTURE COMPLETE:",
        3: r"Step 3 complete|download.*complete|=== DOWNLOAD COMPLETE:",
        4: r"Step 4 complete|clean.*complete|Scale to full data",
        5: r"Step 5 complete|upload.*complete|=== UPLOAD COMPLETE:",
        6: r"Step 6 complete|dbt.*complete|=== DBT COMPLETE:",
        7: r"Step 7 complete|validation.*complete|=== VALIDATION COMPLETE:",
        8: r"Step 8 complete|discover.*complete|=== DISCOVERED IDs",
        9: r"Step 9 complete|metadata.*dev.*complete|=== METADATA REGISTRATION COMPLETE.*dev",
        10: r"Step 10 complete|metadata.*prod.*complete|=== METADATA REGISTRATION COMPLETE.*prod",
        11: r"Step 11 complete|PR.*opened|pull request.*created",
    }

    completed = []
    for step, pattern in step_patterns.items():
        if re.search(pattern, transcript, re.IGNORECASE):
            completed.append(step)

    state["completed_steps"] = completed
    if completed:
        state["current_step"] = max(completed) + 1

    state["env"] = "prod" if 10 in completed else "dev"

    return state


def main():
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, EOFError):
        # No valid payload — allow compaction to proceed
        sys.exit(0)

    transcript = payload.get("transcript", "")
    if not transcript:
        sys.exit(0)

    # Only save state if we appear to be in an onboarding session
    if (
        "DATASET CONTEXT" not in transcript
        and "onboard" not in transcript.lower()
    ):
        sys.exit(0)

    state = extract_state_from_transcript(transcript)

    # Only write if we have a slug — without it the orchestrator cannot resume
    if state["dataset_slug"]:
        state_file = (
            Path(__file__).parents[2]
            / "models"
            / state["dataset_slug"]
            / "onboarding-state.json"
        )
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(json.dumps(state, indent=2, ensure_ascii=False))
        print(
            f"[pre-compact] Saved onboarding state for dataset "
            f"'{state['dataset_slug']}' "
            f"(completed steps: {state['completed_steps']}) "
            f"to {state_file}",
            file=sys.stderr,
        )

    sys.exit(0)


if __name__ == "__main__":
    main()
