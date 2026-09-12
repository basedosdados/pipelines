"""Build the ``igr_projection`` table from the 2023 Intergenerational Report.

The 2023 edition is the only one this table can be built from. The 2021 edition
published chart data but no Word bundle, so its projection and sensitivity tables
exist only inside the PDF; the 2015 edition published neither. Both are excluded
rather than transcribed.

Source is the report's own appendices, not its chart data. Appendix A1
"Projections summary" gives labelled decadal tables with their units stated in the
caption or the row heading; Appendix A4 "Sensitivity analysis" gives the same
measures under six alternative scenarios. The chart-data workbooks cover more
years but carry no chart titles in the 2023 edition, so a series there cannot be
given a unit without reading the report -- which is why they are shipped as
auxiliary files instead of being modelled.

As in ``aggregate``, the units Treasury publishes become value columns rather than
a unit column, so every numeric column carries one measurement unit. A measure
appears in at most one of them.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys

import docx_tables
import releases

DATA_ROOT = pathlib.Path(
    os.environ.get(
        "AU_TREASURY_BUDGET_DATA",
        pathlib.Path.home() / "Downloads" / "au_treasury_budget_data",
    )
)

IGR_DIR = DATA_ROOT / "input" / "igr2023w"
SUMMARY_DOC = IGR_DIR / "2023_IGR_A1_Projections_summary.docx"
SENSITIVITY_DOC = IGR_DIR / "2023_IGR_A4_Sensitivity_analysis.docx"

PERCENT_GDP = "value_percent_gdp"
DOLLARS_PER_PERSON = "value_aud_per_person_real"
PERCENT = "value_percent"
PERSONS_MILLION = "value_persons_million"
YEARS = "value_years"
BIRTHS_PER_WOMAN = "value_births_per_woman"

VALUE_COLUMNS = (
    PERCENT_GDP,
    DOLLARS_PER_PERSON,
    PERCENT,
    PERSONS_MILLION,
    YEARS,
    BIRTHS_PER_WOMAN,
)

COLUMNS = (
    "year",
    "financial_year",
    "igr_edition",
    "scenario",
    "measure_category",
    "measure",
    "source_table",
    *VALUE_COLUMNS,
)

#: Units that a row heading states for itself, e.g. "Real GDP growth (%)".
_HEADING_UNITS: tuple[tuple[str, str], ...] = (
    (r"\(millions?\)", PERSONS_MILLION),
    (r"\(million\)", PERSONS_MILLION),
    (r"\(years\)", YEARS),
    (r"\(\$\)", DOLLARS_PER_PERSON),
    (r"\(%\)", PERCENT),
    (r"\(% of gdp\)", PERCENT_GDP),
)

#: Units for the few rows that state none anywhere -- not in their own heading,
#: not in the group heading above them, not in the table caption. Anything else
#: stops the build rather than being written with a guessed unit.
_MEASURE_UNITS: dict[str, str] = {
    "total_fertility_rate": BIRTHS_PER_WOMAN,
    "old_age_dependency_ratio": PERCENT,
    "population_growth": PERCENT,
    "net_overseas_migration": PERCENT,
    # Keyed on the post-alias name, since _slug applies aliases.
    "participation_rate_male_15_plus": PERCENT,
    "participation_rate_female_15_plus": PERCENT,
}

#: Measures the two appendices name differently. Appendix A4 writes "Real GNI per
#: capita" for A1's "Real GNI per person" and "Labour force participation" for its
#: "Total participation rate 15+"; left unaliased, the baseline cross-check
#: between the appendices would quietly compare nothing.
_MEASURE_ALIASES: dict[str, str] = {
    "real_gni_per_capita": "real_gni_per_person",
    "labour_force_participation": "total_participation_rate_15_plus",
    "primary_cash_balance": "primary_balance",
    "male_15_plus": "participation_rate_male_15_plus",
    "female_15_plus": "participation_rate_female_15_plus",
    "ndis_australian_government": "national_disability_insurance_scheme",
    "aged_and_services_pensions": "age_and_service_pension",
}

#: Group headings that categorise the rows beneath them.
_CATEGORY_OF_GROUP: dict[str, str] = {
    "economic_projections": "economic",
    "economic": "economic",
    "fiscal_projections": "fiscal",
    "spending": "payments",
    "payments_to_individuals": "payments",
    "population": "demographic",
    "life_expectancy_at_birth": "demographic",
}

_SCENARIOS = (
    ("Population", "Higher", "population_higher"),
    ("Population", "Lower", "population_lower"),
    ("Participation", "Higher", "participation_higher"),
    ("Participation", "Lower", "participation_lower"),
    ("Productivity", "Higher", "productivity_higher"),
    ("Productivity", "Lower", "productivity_lower"),
)

BASELINE = "baseline"


class UnknownMeasureError(ValueError):
    """A row heading whose unit cannot be established."""


#: Parenthetical content that declares a unit and therefore does not belong in a
#: measure name. Everything else in parentheses is kept, because it distinguishes
#: measures: dropping it merges "Total payments (excl. interest)" into "Total
#: payments" -- two different projections, silently averaged into one row.
_UNIT_PARENTHETICAL = re.compile(
    r"\(\s*(%|\$|millions?|years|% of gdp|"
    r"contribution to population growth, percentage points)\s*\)",
    re.I,
)


def _slug(text: str) -> str:
    # Row labels use en and em dashes and a curly apostrophe.
    text = text.replace("–", "-").replace("—", "-").replace("’", "")  # noqa: RUF001
    text = _UNIT_PARENTHETICAL.sub(" ", text)
    text = text.replace("(", " ").replace(")", " ")
    text = text.replace("+", " plus ").replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text.lower())
    text = re.sub(r"_+", "_", text).strip("_")
    return _MEASURE_ALIASES.get(text, text)


def _heading_unit(heading: str) -> str | None:
    lowered = heading.lower()
    if "% of gdp" in lowered:
        return PERCENT_GDP
    for pattern, unit in _HEADING_UNITS:
        if re.search(pattern, lowered):
            return unit
    return None


def _caption_unit(caption: str) -> str | None:
    lowered = caption.lower()
    if "real spending per person" in lowered:
        return DOLLARS_PER_PERSON
    if "% of gdp" in lowered:
        return PERCENT_GDP
    return None


def _financial_years(header: list[str]) -> dict[int, str]:
    """Column index -> financial year, from a header row of decadal columns."""
    found: dict[int, str] = {}
    for index, cell in enumerate(header):
        parsed = docx_tables.normalise_year(cell)
        if parsed:
            found[index] = parsed[0]
    return found


def _emit(
    records: dict[tuple, dict],
    *,
    financial_year: str,
    scenario: str,
    category: str,
    measure: str,
    source_table: str,
    unit: str,
    value: float,
) -> None:
    key = (financial_year, scenario, measure)
    record = records.setdefault(
        key,
        {
            "year": releases.financial_year_start(financial_year),
            "financial_year": financial_year,
            "igr_edition": releases.IGR_EDITION,
            "scenario": scenario,
            "measure_category": category,
            "measure": measure,
            "source_table": source_table,
            **{column: None for column in VALUE_COLUMNS},
        },
    )
    if record[unit] is None:
        record[unit] = value
    if source_table not in record["source_table"].split(","):
        record["source_table"] = ",".join(
            sorted(set(record["source_table"].split(",")) | {source_table})
        )


def extract_summary() -> dict[tuple, dict]:
    """Baseline projections from Appendix A1."""
    records: dict[tuple, dict] = {}
    tables = docx_tables.read_tables(str(SUMMARY_DOC))
    for table_index, table in enumerate(tables):
        header = table.rows[0]
        years = _financial_years(header)
        if not years:
            continue
        caption_unit = _caption_unit(table.caption)
        source_table = f"A1.{table_index + 1}"
        # A table whose year columns start at index 2 indents its sub-rows into a
        # second label column; one whose years start at index 1 does not. That
        # distinction decides whether a labelled data row also heads a group.
        first_year_column = min(years)
        has_sub_label_column = first_year_column >= 2
        category = "demographic" if table_index == 0 else "economic"
        group_unit: str | None = None
        group_label = ""
        for row in table.rows[1:]:
            label = row[0].strip()
            sub_label = (
                row[1].strip() if has_sub_label_column and len(row) > 1 else ""
            )
            has_values = any(
                docx_tables.parse_number(row[i]) is not None
                for i in years
                if i < len(row)
            )
            # A row opens a group when it carries no values -- "Fiscal projections
            # (% of GDP)" -- or when it is a labelled row in a table that indents
            # sub-rows, since "Population (millions)" is both a total and the
            # heading its age bands inherit their unit from.
            opens_group = bool(label) and (
                not has_values or has_sub_label_column
            )
            if opens_group:
                group_label = label
                group_unit = _heading_unit(label)
                category = _CATEGORY_OF_GROUP.get(_slug(label), category)
            if not has_values:
                continue
            heading = (
                f"{group_label} {sub_label}".strip() if sub_label else label
            )
            measure = _slug(heading)
            unit = (
                _heading_unit(heading)
                or _MEASURE_UNITS.get(measure)
                or group_unit
                or caption_unit
            )
            if unit is None:
                raise UnknownMeasureError(
                    f"A1 table {table_index}: no unit for {heading!r} "
                    f"(slug {measure!r}). Add it to _MEASURE_UNITS."
                )
            for index, financial_year in years.items():
                if index >= len(row):
                    continue
                value = docx_tables.parse_number(row[index])
                if value is None:
                    continue
                _emit(
                    records,
                    financial_year=financial_year,
                    scenario=BASELINE,
                    category=category,
                    measure=measure,
                    source_table=source_table,
                    unit=unit,
                    value=value,
                )
    return records


def extract_sensitivity() -> dict[tuple, dict]:
    """Alternative-scenario projections from Appendix A4."""
    records: dict[tuple, dict] = {}
    table = None
    for candidate in docx_tables.read_tables(str(SENSITIVITY_DOC)):
        if (
            candidate.rows
            and "Baseline" in candidate.rows[0]
            and len(candidate.rows) > 10
        ):
            table = candidate
            break
    if table is None:
        raise ValueError("A4: sensitivity results table not found")

    # The scenario and direction headers are merged cells -- "Baseline" spans the
    # 2022-23 and 2062-63 columns, "Population" spans Higher and Lower -- so the
    # label must be read from the spanned grid or every second column loses its
    # scenario.
    scenario_row = table.spanned_rows[0]
    direction_row = table.spanned_rows[1]
    year_row = table.rows[2]
    years = _financial_years(year_row)

    columns: list[tuple[int, str, str]] = []
    for index in years:
        group = (
            scenario_row[index].strip() if index < len(scenario_row) else ""
        )
        direction = (
            direction_row[index].strip() if index < len(direction_row) else ""
        )
        if group == "Baseline":
            columns.append((index, BASELINE, years[index]))
            continue
        match = next(
            (
                slug
                for g, d, slug in _SCENARIOS
                if g == group and d == direction
            ),
            None,
        )
        if match is None:
            raise ValueError(
                f"A4 column {index}: unknown scenario {group!r}/{direction!r}"
            )
        columns.append((index, match, years[index]))

    category = "economic"
    group_unit: str | None = None
    for row in table.rows[3:]:
        label = row[0].strip()
        has_values = any(
            docx_tables.parse_number(row[i]) is not None
            for i, _, _ in columns
            if i < len(row)
        )
        if not has_values:
            if label:
                group_unit = _heading_unit(label)
                category = _CATEGORY_OF_GROUP.get(_slug(label), category)
            continue
        measure = _slug(label)
        unit = (
            _heading_unit(label) or _MEASURE_UNITS.get(measure) or group_unit
        )
        if unit is None:
            raise UnknownMeasureError(
                f"A4: no unit for {label!r} (slug {measure!r}). "
                "Add it to _MEASURE_UNITS."
            )
        for index, scenario, financial_year in columns:
            if index >= len(row):
                continue
            value = docx_tables.parse_number(row[index])
            if value is None:
                continue
            _emit(
                records,
                financial_year=financial_year,
                scenario=scenario,
                category=category,
                measure=measure,
                source_table="A4.2",
                unit=unit,
                value=value,
            )
    return records


def check_baseline_agreement(
    summary: dict[tuple, dict], sensitivity: dict[tuple, dict]
) -> list[str]:
    """A4's baseline column must reproduce A1's baseline projections.

    The two appendices are typeset independently, so agreement between them is a
    real check that scenario columns were read in the right order. A4 rounds the
    large dollar figures to four significant figures, so agreement is judged
    proportionally for those and absolutely for percentages.
    """
    failures = []
    compared = 0
    for key, right in sensitivity.items():
        if key[1] != BASELINE or key not in summary:
            continue
        left = summary[key]
        for column in VALUE_COLUMNS:
            a, b = left[column], right[column]
            if a is None or b is None:
                continue
            compared += 1
            tolerance = max(0.051, abs(a) * 0.001)
            if abs(a - b) > tolerance:
                failures.append(
                    f"{key[0]} {key[2]} [{column}]: A1={a} but A4={b}"
                )
    print(
        f"  A1/A4 baseline agreement checks: {compared}, failed: {len(failures)}"
    )
    return failures


def merge(
    summary: dict[tuple, dict], sensitivity: dict[tuple, dict]
) -> list[dict]:
    """Combine the baseline projections with the alternative scenarios."""
    merged = dict(summary)
    for key, record in sensitivity.items():
        if key in merged:
            for column in VALUE_COLUMNS:
                if merged[key][column] is None:
                    merged[key][column] = record[column]
            merged[key]["source_table"] = ",".join(
                sorted(
                    set(merged[key]["source_table"].split(","))
                    | set(record["source_table"].split(","))
                )
            )
        else:
            merged[key] = record
    return sorted(
        merged.values(), key=lambda r: (r["year"], r["scenario"], r["measure"])
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(DATA_ROOT / "output"))
    args = parser.parse_args()

    summary = extract_summary()
    sensitivity = extract_sensitivity()
    print(f"A1 summary rows:     {len(summary)}")
    print(f"A4 sensitivity rows: {len(sensitivity)}")

    failures = check_baseline_agreement(summary, sensitivity)
    if failures:
        print("\nBASELINE DISAGREEMENTS:")
        for failure in failures[:20]:
            print("  ", failure)
        return 1

    merged = dict(summary)
    for key, record in sensitivity.items():
        if key in merged:
            for column in VALUE_COLUMNS:
                if merged[key][column] is None:
                    merged[key][column] = record[column]
            merged[key]["source_table"] = ",".join(
                sorted(
                    set(merged[key]["source_table"].split(","))
                    | set(record["source_table"].split(","))
                )
            )
        else:
            merged[key] = record

    rows = sorted(
        merged.values(), key=lambda r: (r["year"], r["scenario"], r["measure"])
    )
    scenarios = sorted({r["scenario"] for r in rows})
    years = sorted({r["financial_year"] for r in rows})
    measures = sorted({r["measure"] for r in rows})
    print(f"\ntotal rows: {len(rows)}")
    print(f"scenarios:  {scenarios}")
    print(f"years:      {years}")
    print(f"measures:   {len(measures)}")

    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "igr_projection.jsonl").open("w") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    print(f"\nwrote {out / 'igr_projection.jsonl'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
