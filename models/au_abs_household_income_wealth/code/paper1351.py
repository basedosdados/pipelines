"""Appendix tables of ABS working paper 1351.0 (2002).

"Experimental Estimates of the Distribution of Household Wealth, Australia,
1994–2000" is the only ABS source giving household wealth by the age of the
reference person before the Survey of Income and Housing began collecting
wealth in 2003-04. Everything in the body of the paper is a chart; the numbers
exist only in appendix 14.2, as 30 tables in the PDF's text layer.

These estimates are **not comparable with the 6523.0 cubes**. They are modelled
from the national accounts household balance sheet, benchmarked against the
Survey of Income and Housing Costs and the Household Expenditure Survey, and
ABS published them as experimental. They are kept in their own table for that
reason, never merged into ``household_estimate``.

The PDF's column headers wrap over up to five lines, so reading them back from
the text layer is unreliable. The labels are declared here instead and the
parser asserts that each row carries exactly as many values as labels, which
fails loudly if a table's shape ever differs from what is declared.
"""

from __future__ import annotations

import re

COLUMNS = [
    "year",
    "source_table_id",
    "source_table_name",
    "breakdown_type",
    "breakdown_value",
    "measure",
    "measurement_unit",
    "estimate",
]

TABLE_HEADING = re.compile(r"^Table (14\.2\.\d+):\s*(.*)$")
NUMBER = re.compile(r"^-?[\d,]+$")

AGE = [
    "15–24",
    "25–34",
    "35–44",
    "45–54",
    "55–64",
    "65–75",
    "75 and over",
]
DECILE = [
    "Lowest",
    "Second",
    "Third",
    "Fourth",
    "Fifth",
    "Sixth",
    "Seventh",
    "Eighth",
    "Ninth",
    "Tenth",
]
STATE = [
    "New South Wales",
    "Victoria",
    "Queensland",
    "South Australia",
    "Western Australia",
    "Tasmania",
    "Northern Territory",
    "Australian Capital Territory",
]
HOUSEHOLD_TYPE = [
    "Couple only",
    "Couple with dependants aged 0–14 only",
    "Couple with dependants aged 15–24 only",
    "Couple with dependants aged 0–14 and 15–24 only",
    "Lone person only",
    "Lone parent with dependants aged 0–14 only",
    "Lone parent with dependants aged 15–24 only",
    "Other households",
]
HOUSEHOLD_TYPE_AND_AGE = [
    "Couple with dependants aged 0–14 only",
    "Couple with dependants aged 15–24 only",
    "Couple with dependants aged 0–14 and 15–24 only",
    "Lone parent with dependants aged 0–14 only",
    "Lone parent with dependants aged 15–24 only",
    "Single young person",
    "Single middle-aged person",
    "Single older person",
]

AGE_REF = "age_of_reference_person"
AGE_OLDEST = "age_of_oldest_person"

# table id -> (measure, breakdown type, column labels, row mode)
#
# row modes: "year" (each row is a year), "measure_year" (each row is a
# measure and a year, as in "Net Worth 1994"), "benchmark" (each row is a
# benchmarking variant, all for 2000).
TABLES = {
    "14.2.1": ("Average household net worth", AGE_REF, AGE, "year"),
    "14.2.2": ("Median household net worth", AGE_REF, AGE, "year"),
    "14.2.3": ("Average owner-occupied dwelling assets", AGE_REF, AGE, "year"),
    "14.2.4": ("Median owner-occupied dwelling assets", AGE_REF, AGE, "year"),
    "14.2.5": ("Average superannuation assets", AGE_REF, AGE, "year"),
    "14.2.6": ("Median superannuation assets", AGE_REF, AGE, "year"),
    "14.2.7": ("Average", AGE_REF, AGE, "measure_year"),
    "14.2.8": ("Median", AGE_REF, AGE, "measure_year"),
    "14.2.9": ("Average household net worth", AGE_OLDEST, AGE, "year"),
    "14.2.10": ("Median household net worth", AGE_OLDEST, AGE, "year"),
    "14.2.11": ("Average annual household income", AGE_REF, AGE, "year"),
    "14.2.12": ("Median annual household income", AGE_REF, AGE, "year"),
    "14.2.13": (
        "Average household net worth",
        "gross_annual_income_decile",
        DECILE,
        "year",
    ),
    "14.2.14": (
        "Median household net worth",
        "gross_annual_income_decile",
        DECILE,
        "year",
    ),
    "14.2.15": (
        "Average household net worth",
        "household_composition",
        HOUSEHOLD_TYPE,
        "year",
    ),
    "14.2.16": (
        "Median household net worth",
        "household_composition",
        HOUSEHOLD_TYPE,
        "year",
    ),
    "14.2.17": (
        "Average household net worth",
        "household_type_and_age",
        HOUSEHOLD_TYPE_AND_AGE,
        "year",
    ),
    "14.2.18": (
        "Median household net worth",
        "household_type_and_age",
        HOUSEHOLD_TYPE_AND_AGE,
        "year",
    ),
    "14.2.19": (
        "Average gross annual household income",
        "household_type_and_age",
        HOUSEHOLD_TYPE_AND_AGE,
        "year",
    ),
    "14.2.20": (
        "Median gross annual household income",
        "household_type_and_age",
        HOUSEHOLD_TYPE_AND_AGE,
        "year",
    ),
    "14.2.21": (
        "Average household net worth",
        "state_or_territory",
        STATE,
        "year",
    ),
    "14.2.22": (
        "Median household net worth",
        "state_or_territory",
        STATE,
        "year",
    ),
    "14.2.23": (
        "Average owner-occupied dwelling values",
        "state_or_territory",
        STATE,
        "year",
    ),
    "14.2.24": (
        "Median owner-occupied dwelling values",
        "state_or_territory",
        STATE,
        "year",
    ),
    "14.2.25": (
        "Average gross annual household income",
        "state_or_territory",
        STATE,
        "year",
    ),
    "14.2.26": (
        "Median gross annual household income",
        "state_or_territory",
        STATE,
        "year",
    ),
    "14.2.27": (
        "Average household net worth",
        "net_worth_decile",
        DECILE,
        "year",
    ),
    "14.2.28": (
        "Median household net worth",
        "net_worth_decile",
        DECILE,
        "year",
    ),
    "14.2.29": ("Average net worth", AGE_REF, AGE, "benchmark"),
    "14.2.30": ("Median net worth", AGE_REF, AGE, "benchmark"),
}


def _split_row(line: str, width: int):
    """Split a body line into its label and exactly ``width`` values."""
    tokens = line.split()
    values: list[str] = []
    # Take at most ``width`` values from the right: the row label is itself a
    # year in most of these tables, and would otherwise be eaten as a value.
    while tokens and len(values) < width and NUMBER.match(tokens[-1]):
        values.insert(0, tokens.pop())
    if len(values) != width or not tokens:
        return None, None
    return " ".join(tokens), values


def build_rows(text_path: str):
    """Parse appendix 14.2 into long records, one per published number."""
    with open(text_path, encoding="utf-8", errors="replace") as handle:
        lines = handle.read().splitlines()

    starts = {}
    for i, line in enumerate(lines):
        match = TABLE_HEADING.match(line.strip())
        if match and match.group(1) in TABLES:
            starts[match.group(1)] = i

    missing = set(TABLES) - set(starts)
    if missing:
        raise SystemExit(
            f"appendix tables not found in {text_path}: {sorted(missing)}"
        )

    ordered = sorted(starts.items(), key=lambda kv: kv[1])
    rows = []
    for n, (table_id, start) in enumerate(ordered):
        stop = ordered[n + 1][1] if n + 1 < len(ordered) else len(lines)
        measure, breakdown_type, labels, mode = TABLES[table_id]
        found = 0
        for line in lines[start + 1 : stop]:
            label, values = _split_row(line, len(labels))
            if label is None:
                continue
            if mode == "year":
                if not re.fullmatch(r"(19|20)\d{2}", label):
                    continue
                year, row_measure = label, measure
            elif mode == "measure_year":
                parts = label.rsplit(" ", 1)
                if len(parts) != 2 or not re.fullmatch(
                    r"(19|20)\d{2}", parts[1]
                ):
                    continue
                year = parts[1]
                row_measure = f"{measure} {parts[0].lower()}"
            else:
                if label not in ("Unbenchmarked", "Benchmarked"):
                    continue
                year, row_measure = "2000", f"{measure}, {label.lower()}"
            found += 1
            for label_text, value in zip(labels, values, strict=True):
                rows.append(
                    {
                        "year": year,
                        "source_table_id": table_id,
                        "source_table_name": measure,
                        "breakdown_type": breakdown_type,
                        "breakdown_value": label_text,
                        "measure": row_measure,
                        "measurement_unit": "$",
                        "estimate": str(float(value.replace(",", ""))),
                    }
                )
        if not found:
            raise SystemExit(
                f"appendix table {table_id} matched no data rows; its shape"
                " differs from the declared column labels"
            )
    return rows
