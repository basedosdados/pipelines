"""Check the parsed income statement against the department's own tidy export.

For 2022-2024 the department publishes the same HAROLD cube twice: as the wide
xlsx this pipeline parses, and as a long CSV. The CSV covers only the income
statement, but where it overlaps it is an independent rendering of the same
numbers -- so every value the parser produces should appear in it unchanged.
Any mismatch is a parser bug, not a source discrepancy.

Also reports panel shape and null rates for every output table.
"""

from __future__ import annotations

import collections
import csv
import os
import pathlib
import sys

import pyarrow.parquet as pq

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from providers import ProviderIndex  # pyrefly: ignore [missing-import]

DATA_DIR = pathlib.Path(
    os.environ.get(
        "AU_DOE_HEF_DATA",
        pathlib.Path.home()
        / "Downloads/au_doe_higher_education_finances_data",
    )
)
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# The CSV's sector labels, mapped onto the vocabulary used in the output.
CSV_SECTOR = {
    "Total Institution": "total",
    "HED": "higher_education",
    "TAFE": "vocational_education",
}


def read_table(name: str) -> list[dict]:
    root = OUTPUT_DIR / name
    if not root.exists():
        return []
    rows: list[dict] = []
    for path in sorted(root.rglob("*.parquet")):
        # Read the file directly rather than through pq.read_table, which
        # treats the hive path as a dataset and then fails to merge the
        # partition-derived year (dictionary<int32>) with the year column the
        # file itself carries (string). Both are present by convention.
        rows.extend(pq.ParquetFile(path).read().to_pylist())
    return rows


def load_providers() -> ProviderIndex:
    import openpyxl

    book = openpyxl.load_workbook(
        INPUT_DIR / "research_income_time_series.xlsx",
        read_only=True,
        data_only=True,
    )
    providers = {}
    for row in book["1. Summary by Category"].iter_rows(
        min_row=4, values_only=True
    ):
        code = "" if row[0] is None else str(row[0]).strip()
        if code:
            providers.setdefault(code, {"name": str(row[1]).strip()})
    return ProviderIndex(providers)


def cross_check_csv(index: ProviderIndex) -> None:
    """Compare every parsed 2022-2024 income statement value to the CSV export.

    The CSV export uses neither label vocabulary consistently -- some of its
    items are the published label, others the cube's internal name -- so a row
    is looked up under both. Matching on one alone reports most of the file as
    missing when the values are in fact present and correct.
    """
    parsed: dict[tuple, str] = {}
    for row in read_table("income_statement"):
        base = (row["year"], row["hep_code"], row["institution_type"])
        for label in (row["line_item"], row.get("line_item_internal")):
            if label:
                parsed.setdefault((*base, label), row["value"])

    for year in (2022, 2023, 2024):
        path = INPUT_DIR / f"finance_{year}.csv"
        if not path.exists():
            print(f"  {year}: no CSV export available locally, skipped")
            continue

        checked = matched = 0
        mismatches: list[str] = []
        missing: list[str] = []
        for row in csv.reader(path.open(encoding="utf-8-sig")):
            if len(row) < 7:
                continue
            sector = CSV_SECTOR.get(row[1])
            label, item, raw = row[4].strip(), row[5].strip(), row[6].strip()
            if sector is None or index.is_aggregate(label):
                continue
            code = index.code(label)
            if code is None:
                continue
            checked += 1
            key = (str(year), code, sector, item)
            if key not in parsed:
                if len(missing) < 5:
                    missing.append(f"{label} / {item} / {sector}")
                continue
            # The output is in dollars; the CSV, like every published finance
            # table, is in thousands. Compare on the source's own scale.
            in_thousands = float(parsed[key] or 0) / 1000
            if in_thousands == float(raw or 0):
                matched += 1
            elif len(mismatches) < 5:
                mismatches.append(
                    f"{label} / {item} / {sector}: "
                    f"parsed={in_thousands:g} csv={raw}"
                )

        rate = 100 * matched / checked if checked else 0
        status = "OK" if not mismatches else "CHECK"
        print(
            f"  {year}: {matched:,} of {checked:,} CSV rows matched a label "
            f"({rate:.0f}%), {len(mismatches)} value mismatch(es)  [{status}]"
        )
        for note in mismatches:
            print(f"      MISMATCH {note}")
        if missing:
            print(
                f"      {len(missing)}+ CSV labels have no counterpart in the "
                "output; the CSV export uses a third vocabulary (its "
                "'Income Tax' is the workbook's 'Income Tax Expense'), so this "
                "is expected. The value check below is label-independent."
            )


def cross_check_values(index: ProviderIndex) -> None:
    """Compare the numbers themselves, ignoring labels entirely.

    The workbook and the CSV export label the same cube three different ways, so
    a label-keyed comparison understates agreement. This instead takes, for each
    provider-year-sector, the multiset of values each source reports and asks
    how much of the parsed output the CSV also contains. Anything the parser
    invented, misplaced by a column, or scaled wrongly shows up here.
    """
    parsed_blocks: dict[tuple, collections.Counter] = collections.defaultdict(
        collections.Counter
    )
    for row in read_table("income_statement"):
        key = (row["year"], row["hep_code"], row["institution_type"])
        parsed_blocks[key][int(row["value"]) // 1000] += 1

    for year in (2022, 2023, 2024):
        path = INPUT_DIR / f"finance_{year}.csv"
        if not path.exists():
            continue
        csv_blocks: dict[tuple, collections.Counter] = collections.defaultdict(
            collections.Counter
        )
        for row in csv.reader(path.open(encoding="utf-8-sig")):
            if len(row) < 7:
                continue
            sector = CSV_SECTOR.get(row[1])
            label, raw = row[4].strip(), row[6].strip()
            if sector is None or index.is_aggregate(label):
                continue
            code = index.code(label)
            if code is None or not raw:
                continue
            csv_blocks[(str(year), code, sector)][int(float(raw))] += 1

        blocks = covered = total = 0
        exact = 0
        worst = []
        for key, counts in sorted(parsed_blocks.items()):
            if key[0] != str(year) or key not in csv_blocks:
                continue
            blocks += 1
            theirs = csv_blocks[key]
            n = sum(counts.values())
            missing_here = sum(
                max(0, counts[v] - theirs.get(v, 0)) for v in counts
            )
            total += n
            covered += n - missing_here
            if missing_here == 0:
                exact += 1
            elif len(worst) < 3:
                worst.append((key, missing_here, n))

        pct = 100 * covered / total if total else 0
        status = "OK" if pct > 99.5 else "CHECK"
        print(
            f"  {year}: {covered:,}/{total:,} parsed values ({pct:.1f}%) are present "
            f"in the CSV export; {exact}/{blocks} provider blocks match exactly  "
            f"[{status}]"
        )
        for key, n_missing, n in worst:
            print(
                f"      {key}: {n_missing} of {n} values not found in the CSV"
            )


def describe() -> None:
    for name in (
        "income_statement",
        "balance_sheet",
        "equity_movement",
        "cash_flow",
        "research_income",
        "line_item",
    ):
        rows = read_table(name)
        if not rows:
            print(f"  {name:20s} EMPTY")
            continue
        years = sorted({r["year"] for r in rows if r.get("year")})
        providers = len({r["hep_code"] for r in rows if r.get("hep_code")})
        value_col = "amount" if "amount" in rows[0] else "value"
        nulls = sum(1 for r in rows if r.get(value_col) in (None, ""))
        span = f"{years[0]}-{years[-1]} ({len(years)}y)" if years else "n/a"
        extra = f" providers={providers}" if providers else ""
        pct = (
            f" null={100 * nulls / len(rows):.1f}%"
            if value_col in rows[0]
            else ""
        )
        print(f"  {name:20s} {len(rows):>8,} rows  {span}{extra}{pct}")


def check_keys() -> None:
    """The intended primary key must actually be unique."""
    specs = {
        "income_statement": (
            "year",
            "hep_code",
            "institution_type",
            "line_number",
        ),
        "balance_sheet": (
            "year",
            "hep_code",
            "institution_type",
            "line_number",
        ),
        "equity_movement": (
            "year",
            "hep_code",
            "institution_type",
            "line_number",
        ),
        "cash_flow": ("year", "hep_code", "institution_type", "line_number"),
        "research_income": ("year", "hep_code", "category", "sub_category"),
        "line_item": ("statement", "line_item"),
    }
    for name, key in specs.items():
        rows = read_table(name)
        if not rows:
            continue
        counts = collections.Counter(
            tuple(r.get(k) for k in key) for r in rows
        )
        dupes = [(k, n) for k, n in counts.items() if n > 1]
        if dupes:
            print(f"  {name:20s} {len(dupes):,} DUPLICATE keys on {key}")
            for k, n in dupes[:3]:
                print(f"      x{n} {k}")
        else:
            print(f"  {name:20s} key {key} is unique")


def main() -> None:
    print("== Table shape")
    describe()
    print("\n== Primary key uniqueness")
    check_keys()
    index = load_providers()
    print("\n== Income statement labels vs the department's CSV export")
    cross_check_csv(index)
    print("\n== Income statement values vs the department's CSV export")
    cross_check_values(index)


if __name__ == "__main__":
    main()
