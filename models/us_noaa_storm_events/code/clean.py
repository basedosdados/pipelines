"""One-shot bootstrap: clean every year of the Storm Events corpus to parquet.

Reads the already-downloaded gzipped CSVs under ``INPUT`` and writes partitioned,
all-STRING parquet under ``OUTPUT``. The transform itself is imported from the
pipeline module — this script only drives it over the full year range and reports
row counts.

Run: uv run python models/us_noaa_storm_events/code/clean.py
"""

import json

from common import (
    INPUT,
    OUTPUT,
    assert_all_string,
    build_dicionario,
    clean_year,
    list_source_files,
)


def main() -> None:
    listing = list_source_files()
    years = sorted({y for (_, y) in listing})
    print(f"years {years[0]}-{years[-1]} ({len(years)})")

    totals: dict[str, int] = {}
    for year in years:
        counts = clean_year(year, INPUT, OUTPUT, listing)
        for table, n in counts.items():
            totals[table] = totals.get(table, 0) + n
        print(f"  {year}: {counts}", flush=True)

    totals["dicionario"] = build_dicionario(OUTPUT)
    assert_all_string(OUTPUT)

    print("\nrow totals:", json.dumps(totals, indent=2))
    (OUTPUT / "row_counts.json").write_text(json.dumps(totals, indent=2))


if __name__ == "__main__":
    main()
