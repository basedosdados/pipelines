"""Generate the dbt models and schema.yml for us_ffiec_bank_reporting.

Everything is derived from schema_def.py, so a column added there reaches the
SQL, the schema and the architecture CSV in one step.

Regeneration is a TWO-step process. This script emits the models and schema;
the repo's sqlfmt and yamlfix pre-commit hooks then normalise line wrapping and
list style. Run

    python build_dbt.py && pre-commit run --files models/us_ffiec_bank_reporting/schema.yml models/us_ffiec_bank_reporting/*.sql

or pre-commit.ci will push an autofix commit on top of yours, and your next
regeneration will diff against it forever. The from-clause is emitted
pre-collapsed here because that one is easy to match; the rest is left to the
formatters rather than reimplementing their wrapping rules.

Two things here are deliberate and easy to get wrong:

  * Test scoping lives INSIDE each test, never in a model-level `config:` block.
    A model-level `where` does not reach the model's tests, so every test on the
    300-million-row fact table would run unscoped.
  * The scope placeholder is `__most_recent_year_en__`, not
    `__most_recent_year__`. The latter hardcodes the Portuguese partition column
    `ano` and fails here with "Unrecognized name: ano".
"""

from __future__ import annotations

from pathlib import Path

from schema_def import TABLES

DATASET = "us_ffiec_bank_reporting"
MODEL_DIR = Path(__file__).resolve().parents[1]

PARTITION_RANGE = {
    "institution": (2009, 2031),
    "call_report_item": (2009, 2031),
    "holding_company": (1986, 2031),
    "holding_company_item": (1986, 2031),
    "cra_lending": (1996, 2029),
    "cra_assessment_area_tract": (1996, 2029),
    "cra_respondent": (1996, 2029),
}

# Tables large enough that an unscoped test is a multi-terabyte scan.
SCOPED = {
    "call_report_item",
    "holding_company_item",
    "cra_lending",
    "cra_assessment_area_tract",
}

UNIQUE_KEY = {
    "institution": ["year", "quarter", "rssd_id"],
    "call_report_item": ["year", "quarter", "rssd_id", "item_code"],
    "holding_company": ["year", "quarter", "rssd_id"],
    "holding_company_item": ["year", "quarter", "rssd_id", "item_code"],
    "mdrm_item": ["item_code"],
    "cra_lending": [
        "year",
        "respondent_id",
        "agency_id",
        "loan_type",
        "action_taken",
        "state_id",
        "county_id",
        "msa_md_id",
        "assessment_area_id",
        "tract_income_group",
        "report_level",
        "measure_band",
    ],
    "cra_assessment_area_tract": [
        "year",
        "respondent_id",
        "agency_id",
        "county_id",
        "census_tract_id",
        "assessment_area_id",
    ],
    "cra_respondent": ["year", "respondent_id", "agency_id"],
    "dictionary": ["table_id", "column_name", "key"],
}

NOT_NULL = {
    "institution": ["year", "quarter", "rssd_id"],
    "call_report_item": ["year", "quarter", "rssd_id", "item_code", "value"],
    "holding_company": ["year", "quarter", "rssd_id"],
    "holding_company_item": [
        "year",
        "quarter",
        "rssd_id",
        "item_code",
        "value",
    ],
    "mdrm_item": ["item_code", "mnemonic", "item_number"],
    "cra_lending": ["year", "respondent_id", "agency_id", "measure_band"],
    "cra_assessment_area_tract": ["year", "respondent_id", "agency_id"],
    "cra_respondent": ["year", "respondent_id", "agency_id"],
    "dictionary": ["table_id", "column_name", "key", "value"],
}

# Columns that are legitimately sparse, so the 5% not-null floor does not apply.
# Measured on the built tables by validate.py rather than guessed -- see
# reference_measured_ignore_values_drift.
# Columns exempt from the 5% not-null floor. MEASURED on the built tables, not
# guessed: no column in any of the nine tables comes close to the floor. The
# sparsest is cra_lending.report_level at 38% populated (it is blank on rows
# that are not a total), and the CRA geography columns are well populated once
# "NA" is preserved as a real value rather than collapsed to NULL. An
# unnecessary ignore silently weakens the test, so the map stays empty.
# Re-measure with `validate.py` if the shape of a table changes --
# see reference_measured_ignore_values_drift.
IGNORE_SPARSE: dict[str, list[str]] = {
    # Every key applies to the table's whole span, so temporal_coverage is
    # blank throughout -- the convention's "same coverage as the parent table".
    "dictionary": ["temporal_coverage"],
}

# County FIPS codes that appear in these panels but not in the current county
# directory, because the code was retired. Measured against the built tables,
# not guessed:
#   09001-09015  the eight Connecticut counties, replaced by planning regions
#                (09110-09190) for the 2022 vintage
#   12025        Dade County FL, renamed Miami-Dade in 1997 and recoded to 12086
HISTORICAL_FIPS = {
    "county_id": [
        "09001",
        "09003",
        "09005",
        "09007",
        "09009",
        "09011",
        "09013",
        "09015",
        "12025",
    ],
}

CAST = {
    "STRING": "safe_cast({c} as string)",
    "INT64": "safe_cast({c} as int64)",
    "FLOAT64": "safe_cast({c} as float64)",
    "DATE": "safe_cast({c} as date)",
    "DATETIME": "safe_cast({c} as datetime)",
}


def model_sql(table: str) -> str:
    cols = TABLES[table]
    lines = []
    for name, typ, *_ in cols:
        lines.append(f"    {CAST[typ].format(c=name)} {name},")
    lines[-1] = lines[-1].rstrip(",")
    config = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table in PARTITION_RANGE:
        start, end = PARTITION_RANGE[table]
        config.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        },"
        )
    return (
        "{{\n    config(\n"
        + "\n".join(config)
        + "\n    )\n}}\n\n\nselect\n"
        + "\n".join(lines)
        # The repo's sqlfmt pre-commit hook collapses the from-clause onto one
        # line. Emitting it pre-collapsed keeps regeneration idempotent instead
        # of flip-flopping against the hook on every commit.
        + "\nfrom "
        + f'{{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def _scope(table: str) -> list[str]:
    if table not in SCOPED:
        return []
    return ["          config:", "            where: __most_recent_year_en__"]


def schema_yml() -> str:
    out = ["---", "version: 2", "models:"]
    for table, cols in TABLES.items():
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >")
        for line in TABLE_DESCRIPTION[table].split("\n"):
            out.append(f"      {line}")
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            "          combination_of_columns: ["
            + ", ".join(UNIQUE_KEY[table])
            + "]"
        )
        out.extend(_scope(table))
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if table in IGNORE_SPARSE:
            out.append("          ignore_values:")
            for name in sorted(set(IGNORE_SPARSE[table])):
                out.append(f"            - {name}")
        # This test scans every column of the model at compile time, so an
        # unscoped run on the 456-million-row fact table is the single most
        # expensive thing in the project.
        out.extend(_scope(table))
        covered = [c[0] for c in cols if c[3] == "yes"]
        if covered:
            out.append("      - custom_dictionary_coverage_eng:")
            out.append(
                f"          dictionary_model: ref('{DATASET}__dictionary')"
            )
            out.append("          columns_covered_by_dictionary:")
            for name in covered:
                out.append(f"            - {name}")
            out.extend(_scope(table))
        out.append("    columns:")
        for name, _typ, desc, _dic, directory, _unit, _obs, _original in cols:
            out.append(f"      - name: {name}")
            out.append("        description: >")
            out.append(f"          {desc}")
            tests: list[str] = []
            if name in NOT_NULL.get(table, []):
                if table in SCOPED:
                    # A bare `- not_null` on a 456-million-row table scans the
                    # whole column. Column-level tests need the same scope as
                    # the model-level ones; the scope does not inherit.
                    tests.append("          - not_null:")
                    tests.append("              config:")
                    tests.append(
                        "                where: __most_recent_year_en__"
                    )
                else:
                    tests.append("          - not_null")
            if directory and name != "year":
                ref, _, field = directory.partition(":")
                dataset, _, dir_table = ref.partition(".")
                if name in HISTORICAL_FIPS:
                    # The county directory is a single vintage; these panels run
                    # to 40 years and carry FIPS codes that have since been
                    # retired. Naming them beats a blanket failure allowance.
                    tests.append("          - custom_relationships:")
                    tests.append(
                        f"              to: ref('{dataset}__{dir_table}')"
                    )
                    tests.append(f"              field: {field}")
                    tests.append("              ignore_values:")
                    for code in HISTORICAL_FIPS[name]:
                        tests.append(f"                - '{code}'")
                else:
                    tests.append("          - relationships:")
                    tests.append(
                        f"              to: ref('{dataset}__{dir_table}')"
                    )
                    tests.append(f"              field: {field}")
                if table in SCOPED:
                    tests.append("              config:")
                    tests.append(
                        "                where: __most_recent_year_en__"
                    )
            if tests:
                out.append("        tests:")
                out.extend(tests)
    return "\n".join(out) + "\n"


TABLE_DESCRIPTION = {
    "institution": (
        "Roster of every bank that filed a Call Report, one row per institution and\n"
        "reporting quarter. Carries the identifiers that link the bank across\n"
        "regulators: the Federal Reserve RSSD, the FDIC certificate that joins to\n"
        "us_fdic_bankfind, the OCC charter number and the former OTS docket number."
    ),
    "call_report_item": (
        "Every value every bank reported on its quarterly Call Report, one row per\n"
        "institution, quarter and MDRM item code. Long rather than wide because the\n"
        "item set changes every few quarters as lines are added and retired. Join\n"
        "mdrm_item on item_code for the item's name, type and unit."
    ),
    "holding_company": (
        "Roster of every bank holding company that filed financial reports with the\n"
        "Federal Reserve, one row per company and reporting quarter. June and\n"
        "December quarters carry about 4,200 companies and March and September about\n"
        "420, because the FR Y-9SP filed by smaller holding companies is semiannual\n"
        "while the FR Y-9C is quarterly."
    ),
    "holding_company_item": (
        "Every value bank holding companies reported on their financial filings with\n"
        "the Federal Reserve, one row per company, quarter and MDRM item code. It\n"
        "spans two forms: the quarterly consolidated FR Y-9C, whose codes begin with\n"
        "BHCK, BHCA or BHDM, and the semiannual FR Y-9SP filed by smaller holding\n"
        "companies, whose codes begin with BHSP. Join mdrm_item on item_code for the\n"
        "item's name, type, unit and reporting form."
    ),
    "mdrm_item": (
        "The Federal Reserve's Micro Data Reference Manual: the authoritative\n"
        "catalogue of every item code collected on every regulatory report, and the\n"
        "crosswalk that makes call_report_item and holding_company_item readable."
    ),
    "cra_lending": (
        "Small business and small farm lending disclosed under the Community\n"
        "Reinvestment Act, by institution, year, county and census tract income\n"
        "group, split into loan size and borrower revenue bands. Reported only by\n"
        "large institutions, and aggregated by the FFIEC to county level -- the\n"
        "disclosure files carry no per-tract loan amounts."
    ),
    "cra_assessment_area_tract": (
        "The census tracts that make up each institution's CRA assessment areas, one\n"
        "row per institution, year and tract. The tract-level companion to\n"
        "cra_lending, which is only published at county level."
    ),
    "cra_respondent": (
        "The institutions that filed CRA data each year, from the transmittal sheet.\n"
        "The only CRA file carrying the RSSD identifier, and therefore the bridge\n"
        "between the CRA tables and the rest of this dataset."
    ),
    "dictionary": (
        "Dictionary of the coded values used by the columns in this dataset."
    ),
}


def main() -> None:
    for table in TABLES:
        path = MODEL_DIR / f"{DATASET}__{table}.sql"
        path.write_text(model_sql(table), encoding="utf-8")
        print(f"wrote {path.name}")
    (MODEL_DIR / "schema.yml").write_text(schema_yml(), encoding="utf-8")
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
