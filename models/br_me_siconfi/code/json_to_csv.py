"""
Module to convert downloaded SICONFI JSON files to CSV format.

This module processes all JSON files in the input/api directory and converts
them to CSV files, extracting the items from the paginated API response.
"""

import argparse
import csv
import json
from pathlib import Path

# Directory configuration
INPUT_DIR = Path("input/api")
OUTPUT_DIR = Path("input/csv")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_json_file(file_path: Path) -> dict | None:
    """
    Load a JSON file and return the data.

    Handles both old format (direct API response) and new format (with metadata).

    Args:
        file_path: Path to JSON file

    Returns:
        Dictionary with data, or None if error
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            content = json.load(f)

        # Check if it's the new format with metadata
        if isinstance(content, dict) and "data" in content:
            return content["data"]
        # Otherwise assume it's the old format (direct API response)
        return content

    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return None


def extract_items(data: dict) -> list[dict]:
    """
    Extract items from API response.

    The API returns paginated data with structure:
    {
        "items": [...],
        "hasMore": bool,
        "count": int,
        ...
    }

    Args:
        data: API response dictionary

    Returns:
        List of item dictionaries
    """
    if isinstance(data, dict):
        if "items" in data:
            return data["items"]
        # If no items key, might be a single item or different structure
        return [data]
    elif isinstance(data, list):
        return data
    else:
        return []


def get_exercicio_from_file(json_file: Path) -> int | None:
    """
    Extract exercicio (year) from JSON file.

    Args:
        json_file: Path to JSON file

    Returns:
        Year (exercicio) or None if cannot determine
    """
    # Try to extract from filename: dca_{year}_{id_ente}.json
    parts = json_file.stem.split("_")
    if len(parts) >= 3 and parts[0] == "dca":
        try:
            return int(parts[1])
        except ValueError:
            pass

    # Try to get from file content
    try:
        with open(json_file, encoding="utf-8") as f:
            content = json.load(f)
            # Check metadata
            if isinstance(content, dict) and "metadata" in content:
                exercicio = content["metadata"].get("exercicio")
                if exercicio:
                    return exercicio
            # Check data items
            data = (
                content.get("data", content)
                if isinstance(content, dict)
                else content
            )
            if isinstance(data, dict) and "items" in data and data["items"]:
                return data["items"][0].get("exercicio")
    except Exception:
        pass

    return None


def extract_items_from_file(json_file: Path) -> list[dict] | None:
    """
    Extract items from a JSON file.

    Args:
        json_file: Path to JSON file

    Returns:
        List of items or None if error
    """
    data = load_json_file(json_file)
    if data is None:
        return None

    items = extract_items(data)
    return items if items else None


def convert_all_json_to_csv(
    input_dir: Path = INPUT_DIR, output_dir: Path = OUTPUT_DIR
):
    """
    Convert all JSON files in input directory to CSV, grouped by year.

    Creates one CSV file per year containing all municipalities for that year.

    Args:
        input_dir: Directory containing JSON files
        output_dir: Directory to save CSV files
    """
    print("🔄 Converting JSON files to CSV (grouped by year)...")
    print(f"📁 Input directory: {input_dir.absolute()}")
    print(f"📁 Output directory: {output_dir.absolute()}")

    # Find all JSON files
    json_files = list(input_dir.glob("dca_*.json"))
    if not json_files:
        print(f"⚠️  No JSON files found in {input_dir}")
        return

    print(f"📊 Found {len(json_files)} JSON files")

    # Group files by year (exercicio)
    files_by_year: dict[int, list[Path]] = {}
    failed_files = []

    for json_file in sorted(json_files):
        exercicio = get_exercicio_from_file(json_file)
        if exercicio:
            if exercicio not in files_by_year:
                files_by_year[exercicio] = []
            files_by_year[exercicio].append(json_file)
        else:
            failed_files.append(json_file)
            print(f"⚠️  Could not determine year for {json_file.name}")

    if not files_by_year:
        print("❌ No files with valid years found")
        return

    print(
        f"📅 Found data for {len(files_by_year)} year(s): {sorted(files_by_year.keys())}"
    )

    # Process each year
    successful_years = 0
    failed_years = 0
    total_rows = 0

    for exercicio in sorted(files_by_year.keys()):
        print(
            f"\n📅 Processing year {exercicio} ({len(files_by_year[exercicio])} files)..."
        )

        # Collect all items for this year
        all_items = []
        processed_files = 0

        for json_file in files_by_year[exercicio]:
            items = extract_items_from_file(json_file)
            if items:
                all_items.extend(items)
                processed_files += 1
            else:
                print(f"  ⚠️  No items found in {json_file.name}")

        if not all_items:
            print(f"  ❌ No items found for year {exercicio}")
            failed_years += 1
            continue

        # Get all unique keys from all items
        all_keys = set()
        for item in all_items:
            all_keys.update(item.keys())

        # Sort keys for consistent column order
        common_fields = [
            "exercicio",
            "cod_ibge",
            "instituicao",
            "uf",
            "anexo",
            "cod_conta",
            "conta",
            "coluna",
            "valor",
            "rotulo",
            "populacao",
        ]
        ordered_keys = []
        for key in common_fields:
            if key in all_keys:
                ordered_keys.append(key)
                all_keys.remove(key)
        # Add remaining keys in sorted order
        ordered_keys.extend(sorted(all_keys))

        # Write CSV file for this year
        csv_filename = f"dca_{exercicio}.csv"
        csv_path = output_dir / csv_filename

        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f, fieldnames=ordered_keys, extrasaction="ignore"
                )
                writer.writeheader()
                for item in all_items:
                    # Convert None to empty string for CSV
                    row = {
                        k: ("" if v is None else v) for k, v in item.items()
                    }
                    writer.writerow(row)

            print(
                f"  ✅ Created {csv_path.name} with {len(all_items)} rows from {processed_files} municipalities"
            )
            successful_years += 1
            total_rows += len(all_items)

        except Exception as e:
            print(f"  ❌ Error writing CSV {csv_path}: {e}")
            failed_years += 1

    # Summary
    print("\n" + "=" * 70)
    print("Conversion Summary")
    print("=" * 70)
    print(f"✅ Successful years: {successful_years}")
    print(f"❌ Failed years: {failed_years}")
    print(f"📊 Total rows: {total_rows:,}")
    if failed_files:
        print(f"⚠️  Files with errors: {len(failed_files)}")
    print(f"📁 CSV files saved to: {output_dir.absolute()}")


def main():
    """Main entry point for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Convert SICONFI JSON files to CSV format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert all JSON files to CSV
  python json_to_csv.py

  # Convert a specific file
  python json_to_csv.py --file input/api/dca_2013_3302809.json

  # Specify custom directories
  python json_to_csv.py --input-dir input/api --output-dir input/csv
        """,
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Convert a specific JSON file",
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(INPUT_DIR),
        help=f"Input directory with JSON files (default: {INPUT_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(OUTPUT_DIR),
        help=f"Output directory for CSV files (default: {OUTPUT_DIR})",
    )

    args = parser.parse_args()

    if args.file:
        # For single file, we still need to group by year
        # So we'll just process all files in the same directory
        json_file = Path(args.file)
        if not json_file.exists():
            print(f"❌ File not found: {json_file}")
            return
        print(
            "Note: Single file conversion will process all files in the same directory"
        )
        print("    to create year-grouped CSV files.")
        input_dir = json_file.parent
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        convert_all_json_to_csv(input_dir, output_dir)
    else:
        # Convert all files grouped by year
        input_dir = Path(args.input_dir)
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        convert_all_json_to_csv(input_dir, output_dir)


if __name__ == "__main__":
    main()
