"""
Quick script to check download progress without interrupting the main process.
"""

import json
from datetime import datetime
from pathlib import Path

PROGRESS_FILE = Path("input/api/download_progress_api.json")
API_DIR = Path("input/api")


def check_progress():
    """Check and display current download progress."""
    if not PROGRESS_FILE.exists():
        print("❌ Progress file not found. Download may not have started.")
        return

    progress = json.load(open(PROGRESS_FILE))
    completed = len(progress.get("completed", []))
    failed = len(progress.get("failed", []))

    # Count actual files
    json_files = list(API_DIR.glob("dca_*.json"))

    # Expected: ~5,598 municipalities * 14 years (2013-2026) = ~78,372
    expected = 5598 * 14
    remaining = expected - completed
    progress_pct = (completed / expected * 100) if expected > 0 else 0

    # Estimate time
    remaining_seconds = remaining * 1.1  # 1.1 seconds per request
    remaining_hours = remaining_seconds / 3600
    remaining_minutes = (remaining_seconds % 3600) / 60

    print("=" * 70)
    print("DOWNLOAD PROGRESS")
    print("=" * 70)
    print(f"✅ Completed: {completed:,} / {expected:,} ({progress_pct:.2f}%)")
    print(f"❌ Failed: {failed:,}")
    print(f"📁 JSON files on disk: {len(json_files):,}")
    print(f"⏳ Remaining: {remaining:,} downloads")
    print(
        f"⏱️  Estimated remaining time: {remaining_hours:.2f} hours ({remaining_minutes:.0f} minutes)"
    )

    if completed > 0:
        # Calculate average time per download
        # This is a rough estimate
        print(
            f"\n📊 Success rate: {(completed / (completed + failed) * 100):.2f}%"
        )

    # Check by year
    from collections import defaultdict

    by_year = defaultdict(int)
    for item in progress.get("completed", []):
        if isinstance(item, str):
            year = item.split("_")[0]
            by_year[year] += 1

    if by_year:
        print("\n📅 Progress by year:")
        for year in sorted(by_year.keys()):
            count = by_year[year]
            expected_per_year = 5598
            pct = (
                (count / expected_per_year * 100)
                if expected_per_year > 0
                else 0
            )
            print(f"  {year}: {count:,} / {expected_per_year:,} ({pct:.2f}%)")

    print("=" * 70)
    print(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    check_progress()
