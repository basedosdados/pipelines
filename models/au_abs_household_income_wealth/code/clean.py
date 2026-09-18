"""Clean the ABS Household Income and Wealth (6523.0) data cubes to Parquet.

Reads every ``.xlsx`` data cube of one release plus, optionally, the appendix
tables of ABS working paper 1351.0, and writes three tables:

    household_estimate/year=YYYY/       the published summary estimates
    experimental_wealth_estimate/year=YYYY/   working paper 1351.0, 1994–2000
    dictionary/                          labels for the coded columns

Every column is written as a string. Staging is all-string by house convention
and the dbt model ``safe_cast``s each column to its architecture type; writing
typed Parquet here would leave a typed external table that a later all-string
overwrite could not be read against.

Usage:
    python clean.py <input_dir> <output_dir> [--paper <1351.0 text file>]
"""

from __future__ import annotations

import argparse
import collections
import glob
import os
import re
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import paper1351
import semantics
from cubes import join_sheet, parse_workbook

FINANCIAL_YEAR = re.compile(r"^(\d{4})\s*[–—-]\s*(\d{2})$")
# "Table 10.1  INCOME DISTRIBUTION, Age of reference person"
TABLE_TITLE = re.compile(r"^Table\s+[\d.]+\s*(.*)$")

HOUSEHOLD_ESTIMATE_COLUMNS = [
    "year",
    "survey_year",
    "geography",
    "geography_level",
    "source_table_id",
    "source_table_name",
    "breakdown_type",
    "breakdown_value",
    "measure",
    "measurement_unit",
    "estimate",
    "estimate_flag",
    "relative_standard_error",
    "margin_of_error",
]

DICTIONARY_COLUMNS = [
    "table_id",
    "column_name",
    "key",
    "temporal_coverage",
    "value",
]


def financial_year_end(survey_year: str) -> int:
    """ "2019–20" -> 2020, the calendar year the financial year ends in."""
    match = FINANCIAL_YEAR.match(survey_year)
    if not match:
        raise ValueError(f"not a financial year: {survey_year!r}")
    start, end = int(match.group(1)), int(match.group(2))
    return (start // 100) * 100 + end + (100 if end < start % 100 else 0)


def normalise_year(text: str) -> str:
    """Rewrite the ABS en dash so survey years read "2019-20" throughout."""
    match = FINANCIAL_YEAR.match(text)
    return f"{match.group(1)}-{match.group(2)}" if match else text


def normalise_unit(unit: str) -> str:
    return {"$ '000": "$'000"}.get(unit, unit)


def release_survey_year(input_dir: str) -> str:
    """The release a directory holds, taken from its name (``2019-20``)."""
    name = os.path.basename(os.path.normpath(input_dir))
    if FINANCIAL_YEAR.match(name.replace("-", "–")):
        return normalise_year(name.replace("-", "–"))
    raise ValueError(
        f"cannot tell which release {input_dir!r} holds; name the directory"
        " for its financial year, as in 2019-20"
    )


def table_name(title: str) -> str:
    match = TABLE_TITLE.match(title)
    return (match.group(1) if match else title).strip()


def _split_columns(col_path: tuple[str, ...]):
    """Separate a column path into its survey year and its remaining labels."""
    years = [p for p in col_path if FINANCIAL_YEAR.match(p)]
    rest = tuple(p for p in col_path if not FINANCIAL_YEAR.match(p))
    return (years[0] if years else ""), rest


def _join(*parts) -> str:
    """Join label path components, dropping empties."""
    flat: list[str] = []
    for part in parts:
        if isinstance(part, (tuple, list)):
            flat.extend(p for p in part if p)
        elif part:
            flat.append(part)
    return " > ".join(flat)


def _geography(setting: str, col_rest: tuple[str, ...]):
    if setting != "columns":
        return setting, semantics.STATE_LEVEL.get(
            setting, "state or territory"
        )
    for label in col_rest:
        if label in semantics.GEOGRAPHY_LABELS:
            return semantics.GEOGRAPHY_LABELS[label]
    return "", ""


def build_rows(input_dir: str, default_year: str):
    """One record per published estimate across every cube in ``input_dir``."""
    rows = []
    stats = collections.Counter()
    unmapped_row_tops = collections.Counter()

    def sort_key(path):
        match = re.match(r"(\d+)", os.path.basename(path))
        return int(match.group(1)) if match else 999

    files = sorted(glob.glob(os.path.join(input_dir, "*.xlsx")), key=sort_key)
    if not files:
        raise SystemExit(f"no .xlsx cubes found in {input_dir}")

    seen_tables = set()
    for path in files:
        for sheet in parse_workbook(path):
            table_id = sheet.table_id
            seen_tables.add(table_id)
            if table_id not in semantics.TABLES:
                raise SystemExit(
                    f"table {table_id} in {os.path.basename(path)} is not"
                    " declared in semantics.TABLES; the axis roles cannot be"
                    " guessed, so add it there before rerunning"
                )
            mode, fixed_type, geo_setting = semantics.TABLES[table_id]
            name = table_name(sheet.table_name)

            for estimate, rse, moe in join_sheet(sheet):
                survey_year, col_rest = _split_columns(estimate.col_path)
                survey_year = normalise_year(survey_year or default_year)
                geography, level = _geography(geo_setting, col_rest)
                if geo_setting == "columns":
                    col_rest = ()

                path_ = estimate.row_path
                if mode == "columns":
                    breakdown_type = fixed_type
                    breakdown_value = _join(
                        tuple(
                            c
                            for c in col_rest
                            if c not in semantics.TYPE_BANNERS
                        )
                    )
                    measure = _join(estimate.measure_prefix, path_)
                elif mode == "row_leaf" and path_[-1] in (
                    semantics.BREAKDOWN_VALUES
                ):
                    breakdown_type = fixed_type
                    breakdown_value = path_[-1]
                    measure = _join(
                        estimate.measure_prefix, path_[:-1], col_rest
                    )
                elif mode == "row_top" and path_[0] in semantics.ROW_TOP_TYPES:
                    breakdown_type = semantics.ROW_TOP_TYPES[path_[0]]
                    breakdown_value = path_[-1]
                    measure = _join(
                        estimate.measure_prefix, path_[1:-1], col_rest
                    )
                    if not measure:
                        measure = _join(estimate.measure_prefix, col_rest) or (
                            name
                        )
                else:
                    if mode == "row_top":
                        unmapped_row_tops[path_[0]] += 1
                    breakdown_type = "total"
                    breakdown_value = "All households"
                    measure = _join(estimate.measure_prefix, path_, col_rest)

                stats[breakdown_type] += 1
                rows.append(
                    {
                        "year": str(financial_year_end(survey_year)),
                        "survey_year": survey_year,
                        "geography": geography,
                        "geography_level": level,
                        "source_table_id": table_id,
                        "source_table_name": name,
                        "breakdown_type": breakdown_type,
                        "breakdown_value": breakdown_value,
                        "measure": measure,
                        "measurement_unit": normalise_unit(estimate.unit),
                        "estimate": (
                            ""
                            if estimate.value is None
                            else repr(estimate.value)
                        ),
                        "estimate_flag": estimate.flag,
                        "relative_standard_error": (
                            ""
                            if rse is None or rse.value is None
                            else repr(rse.value)
                        ),
                        "margin_of_error": (
                            ""
                            if moe is None or moe.value is None
                            else repr(moe.value)
                        ),
                    }
                )

    missing = set(semantics.TABLES) - seen_tables
    if missing:
        print(
            f"  note: declared but not found in this release: {sorted(missing)}"
        )
    if unmapped_row_tops:
        print("  row-top labels not treated as a breakdown type:")
        for label, n in unmapped_row_tops.most_common():
            print(f"    {n:>6}  {label!r}")
    return rows, stats


def dictionary_rows(rows, paper_rows):
    """Key-to-label rows for every coded column actually written."""
    used_types = {r["breakdown_type"] for r in rows}
    used_types |= {r["breakdown_type"] for r in paper_rows}
    used_flags = {r["estimate_flag"] for r in rows if r["estimate_flag"]}

    out = []
    for table_id, column, keys, labels in (
        (
            "household_estimate",
            "breakdown_type",
            sorted(used_types),
            semantics.BREAKDOWN_TYPE_LABELS,
        ),
        (
            "household_estimate",
            "estimate_flag",
            sorted(used_flags),
            semantics.ESTIMATE_FLAG_LABELS,
        ),
        (
            "experimental_wealth_estimate",
            "breakdown_type",
            sorted({r["breakdown_type"] for r in paper_rows}),
            semantics.BREAKDOWN_TYPE_LABELS,
        ),
    ):
        for key in keys:
            if key not in labels:
                raise SystemExit(
                    f"{column} value {key!r} has no label in semantics.py;"
                    " the dictionary would not cover it"
                )
            out.append(
                {
                    "table_id": table_id,
                    "column_name": column,
                    "key": key,
                    "temporal_coverage": "",
                    "value": labels[key],
                }
            )
    return out


def write_partitioned(rows, columns, output_dir, slug, partition="year"):
    """Write one hive-partitioned all-string Parquet table."""
    if not rows:
        return 0
    by_partition = collections.defaultdict(list)
    for row in rows:
        by_partition[row[partition]].append(row)
    schema = pa.schema([(c, pa.string()) for c in columns if c != partition])
    for value, group in sorted(by_partition.items()):
        target = os.path.join(output_dir, slug, f"{partition}={value}")
        os.makedirs(target, exist_ok=True)
        table = pa.Table.from_pydict(
            {
                c: [r[c] or None for r in group]
                for c in columns
                if c != partition
            },
            schema=schema,
        )
        pq.write_table(
            table, os.path.join(target, "data.parquet"), compression="snappy"
        )
    return len(rows)


def write_flat(rows, columns, output_dir, slug):
    if not rows:
        return 0
    os.makedirs(os.path.join(output_dir, slug), exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in columns])
    table = pa.Table.from_pydict(
        {c: [r[c] or None for r in rows] for c in columns}, schema=schema
    )
    pq.write_table(
        table,
        os.path.join(output_dir, slug, "data.parquet"),
        compression="snappy",
    )
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir")
    parser.add_argument("output_dir")
    parser.add_argument(
        "--paper",
        help="text of ABS working paper 1351.0, from pdftotext -layout",
    )
    args = parser.parse_args()

    default_year = release_survey_year(args.input_dir)
    if default_year != semantics.AUDITED_RELEASE:
        raise SystemExit(
            f"the axis roles in semantics.py were read from the"
            f" {semantics.AUDITED_RELEASE} release, and ABS renumbers its"
            f" cubes between releases, so they cannot be applied to"
            f" {default_year}. Audit that release's table map first."
        )
    print(f"=== cubes: {args.input_dir} (release {default_year}) ===")
    rows, stats = build_rows(args.input_dir, default_year)
    print(f"  estimates: {len(rows):,}")
    for key, n in stats.most_common():
        print(f"    {n:>7,}  {key}")

    paper_rows = []
    if args.paper:
        print(f"=== working paper 1351.0: {args.paper} ===")
        paper_rows = paper1351.build_rows(args.paper)
        print(f"  estimates: {len(paper_rows):,}")

    n = write_partitioned(
        rows, HOUSEHOLD_ESTIMATE_COLUMNS, args.output_dir, "household_estimate"
    )
    print(f"wrote household_estimate: {n:,} rows")
    if paper_rows:
        n = write_partitioned(
            paper_rows,
            paper1351.COLUMNS,
            args.output_dir,
            "experimental_wealth_estimate",
        )
        print(f"wrote experimental_wealth_estimate: {n:,} rows")
    n = write_flat(
        dictionary_rows(rows, paper_rows),
        DICTIONARY_COLUMNS,
        args.output_dir,
        "dictionary",
    )
    print(f"wrote dictionary: {n:,} rows")


if __name__ == "__main__":
    main()
