"""
Generate statistics about the downloaded SICONFI data.

This script analyzes the downloaded JSON files and CSV files to provide
comprehensive statistics about the download process.
"""

import json
from collections import defaultdict
from pathlib import Path

# Directory configuration
API_DIR = Path("input/api")
CSV_DIR = Path("input/csv")
PROGRESS_FILE = API_DIR / "download_progress_api.json"


def get_file_size_mb(file_path: Path) -> float:
    """Get file size in MB."""
    return file_path.stat().st_size / (1024 * 1024)


def analyze_json_files() -> dict:
    """Analyze downloaded JSON files."""
    json_files = list(API_DIR.glob("dca_*.json"))

    if not json_files:
        return {
            "total_files": 0,
            "total_size_mb": 0,
            "files_by_year": {},
            "files_with_metadata": 0,
            "download_dates": [],
        }

    total_size = sum(get_file_size_mb(f) for f in json_files)
    files_by_year = defaultdict(int)
    files_with_metadata = 0
    download_dates = []

    for json_file in json_files:
        # Extract year from filename
        parts = json_file.stem.split("_")
        if len(parts) >= 2 and parts[0] == "dca":
            try:
                year = int(parts[1])
                files_by_year[year] += 1
            except ValueError:
                pass

        # Check for metadata
        try:
            with open(json_file, encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and "metadata" in data:
                    files_with_metadata += 1
                    download_date = data["metadata"].get("download_date")
                    if download_date:
                        download_dates.append(download_date)
        except Exception:
            pass

    return {
        "total_files": len(json_files),
        "total_size_mb": total_size,
        "files_by_year": dict(sorted(files_by_year.items())),
        "files_with_metadata": files_with_metadata,
        "download_dates": download_dates,
    }


def analyze_progress_file() -> dict:
    """Analyze the progress tracking file."""
    if not PROGRESS_FILE.exists():
        return {
            "completed": 0,
            "failed": 0,
            "failed_details": [],
        }

    try:
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)

        completed = len(progress.get("completed", []))
        failed = len(progress.get("failed", []))
        failed_details = progress.get("failed", [])

        return {
            "completed": completed,
            "failed": failed,
            "failed_details": failed_details[:10],  # First 10 failures
        }
    except Exception as e:
        return {
            "error": str(e),
            "completed": 0,
            "failed": 0,
        }


def analyze_csv_files() -> dict:
    """Analyze generated CSV files."""
    csv_files = list(CSV_DIR.glob("dca_*.csv"))

    if not csv_files:
        return {
            "total_files": 0,
            "total_size_mb": 0,
            "files_by_year": {},
            "total_rows": 0,
        }

    total_size = sum(get_file_size_mb(f) for f in csv_files)
    files_by_year = {}
    total_rows = 0

    for csv_file in csv_files:
        # Extract year from filename
        parts = csv_file.stem.split("_")
        if len(parts) >= 2 and parts[0] == "dca":
            try:
                year = int(parts[1])
                # Count rows (subtract 1 for header)
                with open(csv_file, encoding="utf-8") as f:
                    row_count = sum(1 for _ in f) - 1
                files_by_year[year] = {
                    "file": csv_file.name,
                    "size_mb": get_file_size_mb(csv_file),
                    "rows": row_count,
                }
                total_rows += row_count
            except ValueError:
                pass

    return {
        "total_files": len(csv_files),
        "total_size_mb": total_size,
        "files_by_year": dict(sorted(files_by_year.items())),
        "total_rows": total_rows,
    }


def estimate_download_time(completed: int) -> dict:
    """Estimate download time based on rate limit."""
    # API rate limit: 1 request per second
    # Each download = 1 request
    # Plus some overhead for processing
    estimated_seconds = completed * 1.1  # 1.1 seconds per request
    estimated_minutes = estimated_seconds / 60
    estimated_hours = estimated_minutes / 60

    return {
        "estimated_seconds": estimated_seconds,
        "estimated_minutes": estimated_minutes,
        "estimated_hours": estimated_hours,
    }


def print_statistics():
    """Print comprehensive statistics."""
    print("=" * 80)
    print("SICONFI DOWNLOAD STATISTICS")
    print("=" * 80)
    print()

    # JSON files analysis
    print("📁 JSON FILES (Raw Downloads)")
    print("-" * 80)
    json_stats = analyze_json_files()
    print(f"Total files: {json_stats['total_files']:,}")
    print(f"Total size: {json_stats['total_size_mb']:.2f} MB")
    print(f"Files with metadata: {json_stats['files_with_metadata']:,}")

    if json_stats["files_by_year"]:
        print("\nFiles by year:")
        for year in sorted(json_stats["files_by_year"].keys()):
            count = json_stats["files_by_year"][year]
            print(f"  {year}: {count:,} files")

    if json_stats["download_dates"]:
        dates = sorted(json_stats["download_dates"])
        print(f"\nFirst download: {dates[0]}")
        print(f"Last download: {dates[-1]}")

    print()

    # Progress analysis
    print("📊 DOWNLOAD PROGRESS")
    print("-" * 80)
    progress_stats = analyze_progress_file()
    print(f"Completed downloads: {progress_stats['completed']:,}")
    print(f"Failed downloads: {progress_stats['failed']:,}")

    if progress_stats["failed"] > 0:
        success_rate = (
            progress_stats["completed"]
            / (progress_stats["completed"] + progress_stats["failed"])
            * 100
        )
        print(f"Success rate: {success_rate:.2f}%")

    if progress_stats.get("failed_details"):
        print("\nSample failures (first 5):")
        for failure in progress_stats["failed_details"][:5]:
            print(
                f"  - {failure.get('key', 'Unknown')}: {failure.get('error', 'Unknown error')}"
            )

    # Time estimation
    if progress_stats["completed"] > 0:
        time_est = estimate_download_time(progress_stats["completed"])
        print("\nEstimated download time:")
        print(f"  {time_est['estimated_hours']:.2f} hours")
        print(f"  {time_est['estimated_minutes']:.2f} minutes")
        print(f"  {time_est['estimated_seconds']:.0f} seconds")

    print()

    # CSV files analysis
    print("📄 CSV FILES (Processed Data)")
    print("-" * 80)
    csv_stats = analyze_csv_files()
    print(f"Total files: {csv_stats['total_files']:,}")
    print(f"Total size: {csv_stats['total_size_mb']:.2f} MB")
    print(f"Total rows: {csv_stats['total_rows']:,}")

    if csv_stats["files_by_year"]:
        print("\nCSV files by year:")
        for year in sorted(csv_stats["files_by_year"].keys()):
            year_data = csv_stats["files_by_year"][year]
            print(
                f"  {year}: {year_data['rows']:,} rows, {year_data['size_mb']:.2f} MB"
            )

    print()

    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total JSON files: {json_stats['total_files']:,}")
    print(f"Total JSON size: {json_stats['total_size_mb']:.2f} MB")
    print(f"Total CSV files: {csv_stats['total_files']:,}")
    print(f"Total CSV size: {csv_stats['total_size_mb']:.2f} MB")
    print(f"Total data rows: {csv_stats['total_rows']:,}")
    print(f"Completed downloads: {progress_stats['completed']:,}")
    print(f"Failed downloads: {progress_stats['failed']:,}")

    if json_stats["files_by_year"]:
        years = sorted(json_stats["files_by_year"].keys())
        print(
            f"\nYears covered: {years[0]} - {years[-1]} ({len(years)} years)"
        )

    print()


if __name__ == "__main__":
    print_statistics()
