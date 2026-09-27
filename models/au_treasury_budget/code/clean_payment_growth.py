"""Build the ``payment_growth`` table from Budget Paper No. 1, Statement 3.

Treasury publishes one chart -- "Average annual growth in major payments over the
medium term" -- whose *columns are themselves vintages*: the 2026-27 Budget plots
its own medium-term growth rates beside the 2025-26 MYEFO's, and the 2023-24
Budget plots May 2023-24 beside October 2022-23. The chart is therefore already
the comparison exhibit, and flattening it to "the latest numbers" would destroy
the only thing it is for.

Two facts about this chart are not in its spreadsheet and are recorded here from
the statement's own prose:

* **The growth rate is nominal, not real.** The chart note reads "Shows major
  payments that are growing faster than *nominal* GDP over the projection
  period". Nothing in the chart or its data is deflated.
* **The projection window is per column, not per chart.** The note spells it out:
  "Growth rate for MYEFO is from 2025-26 to 2035-36. Growth rate for the 2026-27
  Budget is from 2026-27 to 2036-37." The window is the plotting release's own
  financial year through that year plus ten -- except for
  ``ndis_medium_term_only``, which the same note gives a different window again.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import pathlib
import sys
import zipfile

import openpyxl
import releases

DATA_ROOT = pathlib.Path(
    os.environ.get(
        "AU_TREASURY_BUDGET_DATA",
        pathlib.Path.home() / "Downloads" / "au_treasury_budget_data",
    )
)

#: Where the chart lives in each release's chart-data archive. The sheet name is
#: not stable -- it is "3.8", "3.08", "C3.08", "C3.10", "3.4" and "C3.6" across
#: eight releases -- so it is named per release rather than pattern-matched.
CHART_LOCATIONS: dict[str, tuple[str, str]] = {
    "budget_2026_27": ("bp1-bs3.xlsx", "3.8"),
    "budget_2025_26": ("bp1-bs3.xlsx", "3.08"),
    "budget_2024_25": ("bp1-bs3.xlsx", "3.08"),
    "budget_2023_24": ("bp1-bs3.xlsx", "C3.08"),
    "budget_2022_23_october": ("bp1-bs3.xlsx", "C3.10"),
    "myefo_2025_26": ("part-3-chart-data.xlsx", "C3.7"),
    "myefo_2024_25": ("part-3-chart-data.xlsx", "C3.6"),
    "myefo_2023_24": ("part-3-chart-data.xlsx", "3.4"),
}

#: Releases that appear only as a comparison series, never as a source of data.
#: The 2025 Pre-election Economic and Fiscal Outlook is plotted by the 2025-26
#: MYEFO but publishes no chart data of its own.
REFERENCED_RELEASES: dict[str, tuple[str, str, str]] = {
    "pefo_2025": ("2025 PEFO", "pre_election_fiscal_outlook", "2025-26"),
}

#: Series heading in the spreadsheet -> the release it reports. Headings carry
#: stray spaces and inconsistent capitalisation, so they are matched on a
#: whitespace-collapsed, lower-cased form.
SERIES_TO_RELEASE: dict[str, str] = {
    "2026-27 budget": "budget_2026_27",
    "2025-26 myefo": "myefo_2025_26",
    "2025-26 budget": "budget_2025_26",
    "2025 pefo": "pefo_2025",
    "2024-25 myefo": "myefo_2024_25",
    "2024-25 budget": "budget_2024_25",
    "2023-24 myefo": "myefo_2023_24",
    "may 2023-24 budget": "budget_2023_24",
    "october 2022-23 budget": "budget_2022_23_october",
    # The October 2022-23 Budget plots a single unlabelled series -- its own.
    "cagr programs": "budget_2022_23_october",
}

#: Payment programs, normalised. "Interest" and "PDI" are the same thing: the
#: 2026-27 Budget renamed the series to public debt interest.
PROGRAMS: dict[str, str] = {
    "age pension": "age_pension",
    "medical benefits": "medical_benefits",
    "aged care": "aged_care",
    "defence": "defence",
    "hospitals": "hospitals",
    "ndis": "ndis",
    "ndis medium term only": "ndis_medium_term_only",
    "interest": "public_debt_interest",
    "pdi": "public_debt_interest",
    "child care subsidy": "child_care_subsidy",
}

#: Medium-term projection windows that depart from "plotting release's financial
#: year, plus ten". Keyed by ``(source release, series release, program)`` and
#: taken verbatim from the chart note of the release that published them.
PERIOD_OVERRIDES: dict[tuple[str, str, str], tuple[int, int]] = {
    # 2026-27 Budget, Chart 3.8 note: "Growth rate for NDIS medium term only is
    # from 2029-30 to 2036-37 in Budget and from 2028-29 to 2035-36 in MYEFO."
    ("budget_2026_27", "budget_2026_27", "ndis_medium_term_only"): (
        2029,
        2036,
    ),
    ("budget_2026_27", "myefo_2025_26", "ndis_medium_term_only"): (2028, 2035),
}

#: Length of the medium-term projection window, in years beyond its first year.
#: Verified against the chart note or title of the 2022-23 October, 2024-25,
#: 2025-26 and 2026-27 Budgets, each of which states its window explicitly.
MEDIUM_TERM_SPAN = 10

COLUMNS = (
    "year",
    "source_release_id",
    "source_release_label",
    "series_release_id",
    "series_release_label",
    "payment_program",
    "projection_period_start_year",
    "projection_period_end_year",
    "growth_basis",
    "average_annual_growth_percent",
)


def _release_label(release_id: str) -> tuple[str, str, str]:
    if release_id in REFERENCED_RELEASES:
        return REFERENCED_RELEASES[release_id]
    release = releases.by_id(release_id)
    return release.label, release.release_type, release.release_financial_year


def _normalise(text: object) -> str:
    return " ".join(str(text or "").split()).strip().lower()


def _read_sheet(archive: pathlib.Path, member: str, sheet: str) -> list[list]:
    with zipfile.ZipFile(archive) as bundle:
        names = [n for n in bundle.namelist() if n.endswith(member)]
        if not names:
            raise FileNotFoundError(f"{archive}: no member ending {member!r}")
        workbook = openpyxl.load_workbook(
            io.BytesIO(bundle.read(names[0])), read_only=True, data_only=True
        )
    if sheet not in workbook.sheetnames:
        raise KeyError(
            f"{archive}:{member} has no sheet {sheet!r} -- the chart moved. "
            f"Sheets present: {workbook.sheetnames}"
        )
    return [list(row) for row in workbook[sheet].iter_rows(values_only=True)]


def extract_release(source_release_id: str) -> list[dict]:
    member, sheet = CHART_LOCATIONS[source_release_id]
    archive = DATA_ROOT / "input" / "chart_data" / f"{source_release_id}.zip"
    rows = _read_sheet(archive, member, sheet)

    # The header is the first row naming at least one known series. Sheets vary:
    # some open with a "Website Chart Data" banner, some with an "X Values" stub.
    header_index = None
    for index, row in enumerate(rows):
        if any(_normalise(cell) in SERIES_TO_RELEASE for cell in row[1:]):
            header_index = index
            break
    if header_index is None:
        raise ValueError(
            f"{source_release_id}: no header row naming a known release in "
            f"{member}:{sheet}. Series headings seen: "
            f"{[[_normalise(c) for c in r[1:] if c] for r in rows[:4]]}"
        )

    header = rows[header_index]
    series = [
        (index, SERIES_TO_RELEASE[_normalise(cell)])
        for index, cell in enumerate(header)
        if index > 0 and _normalise(cell) in SERIES_TO_RELEASE
    ]

    source_label, _, _ = _release_label(source_release_id)
    source_year = releases.financial_year_start(
        _release_label(source_release_id)[2]
    )

    out: list[dict] = []
    for row in rows[header_index + 1 :]:
        program_key = _normalise(row[0] if row else None)
        if not program_key:
            continue
        if program_key not in PROGRAMS:
            raise KeyError(
                f"{source_release_id}: unknown payment program {row[0]!r}. Add it "
                "to PROGRAMS -- an unmapped program must not be dropped silently."
            )
        program = PROGRAMS[program_key]
        for index, series_release_id in series:
            if index >= len(row):
                continue
            value = row[index]
            if not isinstance(value, (int, float)):
                continue
            series_label, _, series_financial_year = _release_label(
                series_release_id
            )
            override = PERIOD_OVERRIDES.get(
                (source_release_id, series_release_id, program)
            )
            if override:
                start_year, end_year = override
            else:
                start_year = releases.financial_year_start(
                    series_financial_year
                )
                end_year = start_year + MEDIUM_TERM_SPAN
            out.append(
                {
                    "year": source_year,
                    "source_release_id": source_release_id,
                    "source_release_label": source_label,
                    "series_release_id": series_release_id,
                    "series_release_label": series_label,
                    "payment_program": program,
                    "projection_period_start_year": start_year,
                    "projection_period_end_year": end_year,
                    "growth_basis": "nominal",
                    "average_annual_growth_percent": round(float(value), 4),
                }
            )
    return out


def check_cross_release_agreement(rows: list[dict]) -> list[str]:
    """Where two releases plot the same vintage, they must agree.

    The 2024-25 MYEFO re-plots the 2024-25 Budget's growth rates, and the 2024-25
    Budget plots them itself. Comparing the two is a free check that the series
    columns were attributed to the right release -- a swapped pair of columns
    parses perfectly and is invisible any other way.
    """
    index: dict[tuple[str, str], list[tuple[str, float]]] = {}
    for row in rows:
        key = (row["series_release_id"], row["payment_program"])
        index.setdefault(key, []).append(
            (row["source_release_id"], row["average_annual_growth_percent"])
        )

    failures = []
    compared = 0
    for (series_release_id, program), entries in sorted(index.items()):
        if len(entries) < 2:
            continue
        compared += 1
        values = [value for _, value in entries]
        # The republished figure is rounded to one decimal in some editions and
        # to four in others, so agreement is judged at one decimal place.
        if max(values) - min(values) > 0.05:
            failures.append(
                f"{series_release_id} {program}: "
                + ", ".join(f"{src}={val}" for src, val in entries)
            )
    print(
        f"  cross-release agreement checks: {compared}, failed: {len(failures)}"
    )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(DATA_ROOT / "output"))
    args = parser.parse_args()

    all_rows: list[dict] = []
    for source_release_id in CHART_LOCATIONS:
        rows = extract_release(source_release_id)
        series = sorted({r["series_release_id"] for r in rows})
        print(f"{source_release_id:24s} rows={len(rows):3d} series={series}")
        all_rows.extend(rows)

    print(f"\ntotal rows: {len(all_rows)}")
    failures = check_cross_release_agreement(all_rows)
    if failures:
        print("\nAGREEMENT FAILURES:")
        for failure in failures:
            print("  ", failure)
        return 1

    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "payment_growth.jsonl").open("w") as handle:
        for row in all_rows:
            handle.write(json.dumps(row) + "\n")
    print(f"\nwrote {out / 'payment_growth.jsonl'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
