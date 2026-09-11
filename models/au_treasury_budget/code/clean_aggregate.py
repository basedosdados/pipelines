"""Build the ``aggregate`` table from the historical-data statements.

One row per ``(release, financial year, sector, measure)``; the four units
Treasury publishes become four value columns rather than a unit column, so every
numeric column carries a single measurement unit as the house convention
requires, and the ``$m`` / ``% of GDP`` / real-per-capita views of one measure sit
on one row instead of three.

The vintage dimension is the point. Each release republishes the whole series
back to 1970-71 on its own basis, and an earlier release's numbers are never
overwritten by a later one: they were correct as at their own date, and the
2022-23 October Budget disagreeing with the 2023-24 Budget about 2025-26 is the
exhibit, not an error to reconcile away.
"""

from __future__ import annotations

import argparse
import collections
import dataclasses
import json
import os
import pathlib
import sys
from collections.abc import Iterator

import docx_tables
import measures
import releases

DATA_ROOT = pathlib.Path(
    os.environ.get(
        "AU_TREASURY_BUDGET_DATA",
        pathlib.Path.home() / "Downloads" / "au_treasury_budget_data",
    )
)

#: Value columns, in output order. Keyed by the unit slugs in ``measures``.
VALUE_COLUMNS = (
    measures.DOLLARS_MILLION,
    measures.PERCENT_GDP,
    measures.PERCENT_REAL_GROWTH,
    measures.DOLLARS_PER_PERSON,
)

COLUMNS = (
    "year",
    "financial_year",
    "release_id",
    "release_label",
    "release_type",
    "release_financial_year",
    "sector",
    "measure",
    "estimate_type",
    "source_tables",
    "value_aud_million",
    "value_percent_gdp",
    "value_percent_real_growth",
    "value_aud_per_person_real",
)

#: Accounting identities that must hold within a release-year, used to prove that
#: a column was labelled correctly rather than merely parsed. Each entry is
#: ``(total, parts, tolerance)`` in $m.
IDENTITIES = (
    ("total_receipts", ("taxation_receipts", "non_taxation_receipts"), 1.0),
    ("total_revenue", ("taxation_revenue", "non_taxation_revenue"), 1.0),
)


def releases_with_historical():
    """Re-exported so clean.py has one import for the aggregate build."""
    return releases.releases_with_historical()


def _historical_path(release: releases.Release) -> pathlib.Path:
    return DATA_ROOT / "input" / "historical" / f"{release.release_id}.docx"


@dataclasses.dataclass
class _LogicalTable:
    """One historical table, after the halves Word split it into are rejoined."""

    signature: tuple[str, ...]
    width: int
    caption: str = ""
    number: tuple[str, int] | None = None
    rows: list[list[str]] = dataclasses.field(default_factory=list)


def _logical_tables(
    release: releases.Release, path: str
) -> Iterator[_LogicalTable]:
    """Rejoin each historical table from the halves Word splits it into.

    A 55-year table does not fit on a page, so Treasury splits it and repeats the
    caption over the second half as "... (continued)". Word attaches that caption
    paragraph to the *second* table, leaving the first half captionless -- so
    trusting per-table captions silently drops roughly half of every series, and
    does so without error, because what remains parses perfectly.

    Consecutive tables sharing an identical column-label signature are therefore
    one logical table, and the number is taken from whichever half carries the
    caption.
    """
    groups: list[_LogicalTable] = []
    for table in docx_tables.read_tables(path):
        if not table.data_rows:
            continue
        signature = tuple(table.column_labels())
        if groups and groups[-1].signature == signature:
            group = groups[-1]
        else:
            group = _LogicalTable(signature=signature, width=table.width)
            groups.append(group)
        group.rows.extend(table.data_rows)
        if table.number and group.number is None:
            group.number = table.number
        if table.caption and not group.caption:
            group.caption = table.caption

    for group in groups:
        if group.number is not None:
            yield group


def extract_release(release: releases.Release) -> list[dict]:
    """Long-form observations from one release's historical statement."""
    path = _historical_path(release)
    if not path.exists():
        raise FileNotFoundError(
            f"{release.release_id}: {path} is missing. Run download.py first."
        )

    seen_numbers: set[int] = set()
    cells: dict[tuple, dict] = {}

    for group in _logical_tables(release, str(path)):
        assert group.number is not None
        prefix, number = group.number
        if prefix != release.historical_prefix:
            continue
        has_values = [
            any(
                docx_tables.parse_number(row[index]) is not None
                for row in group.rows
                if index < len(row)
            )
            for index in range(group.width)
        ]
        columns = measures.resolve_columns(
            release_id=release.release_id,
            table_number=number,
            caption=group.caption,
            labels=list(group.signature),
            has_values=has_values,
        )
        seen_numbers.add(number)

        for row in group.rows:
            parsed = docx_tables.normalise_year(row[0])
            if parsed is None:
                continue
            financial_year, is_estimate = parsed
            for column in columns:
                if column.index >= len(row):
                    continue
                value = docx_tables.parse_number(row[column.index])
                if value is None:
                    continue
                key = (financial_year, column.sector, column.measure)
                record = cells.setdefault(
                    key,
                    {
                        "financial_year": financial_year,
                        "sector": column.sector,
                        "measure": column.measure,
                        "estimate_type": "estimate"
                        if is_estimate
                        else "outcome",
                        "source_tables": set(),
                        **{unit: None for unit in VALUE_COLUMNS},
                    },
                )
                record["source_tables"].add(f"{prefix}.{number}")
                existing = record[column.unit]
                if existing is not None and abs(existing - value) > 1e-9:
                    raise ValueError(
                        f"{release.release_id} {financial_year} {column.measure} "
                        f"[{column.unit}]: table {prefix}.{number} says {value} but "
                        f"an earlier table said {existing}. Two historical tables "
                        "disagree about the same measure; this needs a human."
                    )
                record[column.unit] = value
                # An estimate marker anywhere for a year applies to the year.
                if is_estimate:
                    record["estimate_type"] = "estimate"

    missing = set(range(1, 12)) - seen_numbers
    if missing:
        raise ValueError(
            f"{release.release_id}: historical tables {sorted(missing)} were not "
            f"found with prefix {release.historical_prefix!r}. The statement "
            "number or caption format has changed."
        )

    rows = []
    for record in cells.values():
        rows.append(
            {
                "year": releases.financial_year_start(
                    record["financial_year"]
                ),
                "financial_year": record["financial_year"],
                "release_id": release.release_id,
                "release_label": release.label,
                "release_type": release.release_type,
                "release_financial_year": release.release_financial_year,
                "sector": record["sector"],
                "measure": record["measure"],
                "estimate_type": record["estimate_type"],
                "source_tables": ",".join(sorted(record["source_tables"])),
                "value_aud_million": record[measures.DOLLARS_MILLION],
                "value_percent_gdp": record[measures.PERCENT_GDP],
                "value_percent_real_growth": record[
                    measures.PERCENT_REAL_GROWTH
                ],
                "value_aud_per_person_real": record[
                    measures.DOLLARS_PER_PERSON
                ],
            }
        )
    rows.sort(key=lambda r: (r["year"], r["sector"], r["measure"]))
    return rows


def check_identities(rows: list[dict]) -> list[str]:
    """Verify accounting identities that prove the column labelling is right.

    A mislabelled column parses perfectly and produces a plausible number, so
    parsing cleanly proves nothing. Total receipts equalling taxation plus
    non-taxation receipts, on every year of every release, does.
    """
    index: dict[tuple, float] = {}
    for row in rows:
        value = row["value_aud_million"]
        if value is not None:
            index[
                (
                    row["release_id"],
                    row["financial_year"],
                    row["sector"],
                    row["measure"],
                )
            ] = value

    failures = []
    checked = 0
    for total_name, part_names, tolerance in IDENTITIES:
        keys = [k for k in index if k[3] == total_name]
        for key in keys:
            release_id, financial_year, sector, _ = key
            found = [
                index.get((release_id, financial_year, sector, part))
                for part in part_names
            ]
            if any(part is None for part in found):
                continue
            parts = [part for part in found if part is not None]
            checked += 1
            if abs(index[key] - sum(parts)) > tolerance:
                failures.append(
                    f"{release_id} {financial_year} {sector}: {total_name}="
                    f"{index[key]:,.0f} but {' + '.join(part_names)}="
                    f"{sum(parts):,.0f}"
                )
    print(f"  identity checks run: {checked}, failed: {len(failures)}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(DATA_ROOT / "output"))
    args = parser.parse_args()

    all_rows: list[dict] = []
    for release in releases.releases_with_historical():
        rows = extract_release(release)
        years = sorted({r["financial_year"] for r in rows})
        estimates = sorted(
            {
                r["financial_year"]
                for r in rows
                if r["estimate_type"] == "estimate"
            }
        )
        print(
            f"{release.release_id:24s} rows={len(rows):5d} "
            f"years={years[0]}..{years[-1]} ({len(years)}) "
            f"estimates={estimates[0] if estimates else '-'}..{estimates[-1] if estimates else '-'}"
        )
        all_rows.extend(rows)

    print(f"\ntotal rows: {len(all_rows):,}")
    by_measure = collections.Counter(r["measure"] for r in all_rows)
    print(f"distinct measures: {len(by_measure)}")

    failures = check_identities(all_rows)
    if failures:
        print("\nIDENTITY FAILURES:")
        for failure in failures[:20]:
            print("  ", failure)
        return 1

    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "aggregate.jsonl").open("w") as handle:
        for row in all_rows:
            handle.write(json.dumps(row) + "\n")
    print(f"\nwrote {out / 'aggregate.jsonl'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
