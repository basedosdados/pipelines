"""Generate schema.yml for us_fdic_bankfind from the architecture CSVs.

Test scoping is deliberate.  `financials_indicator` holds ~3.25 billion rows and
`financials` is 290 columns wide, and
`not_null_proportion_multiple_columns` expands to every column at compile time,
so both are scoped to the most recent year.  Running them unscoped scans the
whole table and burns a large slice of the daily BigQuery byte quota for no
extra signal.
"""

from __future__ import annotations

import csv
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
DATASET = "us_fdic_bankfind"

KEYS = {
    "institution": ["cert"],
    "indicator": ["indicator_id"],
    "financials": ["year", "quarter", "cert"],
    "financials_indicator": ["year", "quarter", "cert", "indicator_id"],
}
NOT_NULL = {
    "institution": ["cert"],
    "indicator": ["indicator_id"],
    "financials": ["year", "quarter", "report_date", "cert"],
    "financials_indicator": [
        "year",
        "quarter",
        "report_date",
        "cert",
        "indicator_id",
    ],
}
# tables large enough that an unscoped test is a full-table scan
SCOPED = {"financials", "financials_indicator"}

RELATIONSHIPS = {
    ("financials", "cert"): (f"{DATASET}__institution", "cert"),
    ("financials_indicator", "cert"): (f"{DATASET}__institution", "cert"),
    ("financials_indicator", "indicator_id"): (
        f"{DATASET}__indicator",
        "indicator_id",
    ),
}

DESCRIPTIONS = {
    "institution": (
        "Directory of every institution the FDIC has registered, active or "
        "closed, with its identifiers, charter and supervisory classification, "
        "location, key dates and a snapshot of its latest reported financials. "
        "One row per FDIC certificate number."
    ),
    "indicator": (
        "Dictionary of the quarterly line items the FDIC reports, giving each "
        "one a readable name, the FDIC's definition, the unit its values are "
        "expressed in, and the matching column in the financials table when it "
        "has one. Decodes financials_indicator."
    ),
    "financials": (
        "Quarterly Call Report financials for every FDIC-insured institution, "
        "covering the balance sheet, income statement, asset quality, capital "
        "and the FDIC's performance ratios. One row per institution and "
        "quarter, holding the headline line items; the complete set of line "
        "items is in financials_indicator. Dollar amounts are in whole dollars, "
        "converted from the thousands the FDIC publishes."
    ),
    "financials_indicator": (
        "Every quarterly line item the FDIC reports, in long form: one row per "
        "institution, quarter and line item. Covers all reported items rather "
        "than the headline selection in financials, at the cost of needing a "
        "join to the indicator table to read. Rows exist only where the "
        "institution reported the item."
    ),
}


def model(table: str) -> dict:
    with (ARCH / f"{table}.csv").open() as handle:
        rows = list(csv.DictReader(handle))

    tests: list = [
        {
            "dbt_utils.unique_combination_of_columns": {
                "combination_of_columns": KEYS[table]
            }
        },
        {"not_null_proportion_multiple_columns": {"at_least": 0.05}},
    ]
    entry: dict = {
        "name": f"{DATASET}__{table}",
        "description": DESCRIPTIONS[table],
        "tests": tests,
    }
    if table in SCOPED:
        entry["config"] = {"where": "__most_recent_year__"}

    columns = []
    for row in rows:
        column: dict = {"name": row["name"], "description": row["description"]}
        column_tests: list = []
        if row["name"] in NOT_NULL[table]:
            column_tests.append("not_null")
        target = RELATIONSHIPS.get((table, row["name"]))
        if target:
            column_tests.append(
                {
                    "relationships": {
                        "to": f"ref('{target[0]}')",
                        "field": target[1],
                    }
                }
            )
        if column_tests:
            column["tests"] = column_tests
        columns.append(column)
    entry["columns"] = columns
    return entry


class Dumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def _str_presenter(dumper, data):
    style = ">" if len(data) > 70 else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


Dumper.add_representer(str, _str_presenter)

if __name__ == "__main__":
    order = ["institution", "indicator", "financials", "financials_indicator"]
    document = {"version": 2, "models": [model(t) for t in order]}
    text = yaml.dump(document, Dumper=Dumper, sort_keys=False, width=88)
    # dbt expects ref() unquoted inside the relationships test
    text = (
        text.replace("to: ref(", "to: ref(")
        .replace("'ref(", "ref(")
        .replace(")'", ")")
    )
    (HERE.parent / "schema.yml").write_text("---\n" + text)
    print("wrote schema.yml")
