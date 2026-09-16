#!/usr/bin/env python3
"""Generate dbt SQL models + schema.yml for us_bls_atus from the architecture CSVs.

Usage:
    uv run python models/us_bls_atus/code/build_dbt_files.py
"""

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
MODELS = ROOT  # .sql + schema.yml live in models/us_bls_atus/
DATASET = "us_bls_atus"
CAST = {"STRING": "string", "INT64": "int64", "FLOAT64": "float64"}

# per-table: table description + logical key (also the not_null / unique columns)
META = {
    "respondent": (
        "ATUS respondent file: one record per interviewed respondent per year, "
        "with labor force status, earnings, the ATUS final weight, and derived "
        "time-use totals.",
        ["year", "tucaseid"],
    ),
    "roster": (
        "ATUS household roster: age, sex, and relationship to the respondent for each "
        "household member and the respondent's nonhousehold children under 18.",
        ["year", "tucaseid", "tulineno"],
    ),
    "cps": (
        "ATUS-CPS file: Current Population Survey demographics for each household member, "
        "collected 2 to 5 months before the ATUS interview.",
        ["year", "tucaseid", "tulineno"],
    ),
    "activity": (
        "ATUS activity file: one record per activity episode in the 24-hour diary, "
        "with the activity code, start/stop times, location, and secondary "
        "childcare/eldercare information.",
        ["year", "tucaseid", "tuactivity_n"],
    ),
    "who": (
        "ATUS who file: the people who were present during each activity episode.",
        ["year", "tucaseid", "tuactivity_n", "tuwho_code", "tulineno"],
    ),
    "activity_summary": (
        "ATUS activity summary reshaped to long form: total minutes each "
        "respondent spent on each six-digit activity code on the diary day "
        "(only non-zero durations are stored).",
        ["year", "tucaseid", "activity_code"],
    ),
    "eldercare_roster": (
        "ATUS eldercare roster (2011 onward): one record per eldercare "
        "recipient, with the recipient's age, relationship, and duration of "
        "care provided.",
        ["year", "tucaseid", "tueclno"],
    ),
    "replicate_weights": (
        "ATUS replicate final weights: 160 replicate weights per case, used "
        "for variance and standard-error estimation.",
        ["year", "tucaseid"],
    ),
    "pandemic_replicate_weights": (
        "ATUS pandemic-method replicate final weights (2019-2020): "
        "160 replicate weights per case under the 2020 weighting "
        "method.",
        ["year", "tucaseid"],
    ),
    "case_history": (
        "ATUS case history and survey methodology file (2005 onward): interview "
        "outcome codes, reported-activity counts, and interviewer/case "
        "identifiers.",
        ["year", "tucaseid"],
    ),
}
# columns with a clean single-column directory FK -> (directory model, field)
DIR_REL = {
    "year": ("br_bd_diretorios_data_tempo__ano", "ano.ano", False),
    "gestfips": (
        "br_bd_diretorios_us__state",
        "id_state",
        True,
    ),  # custom_relationships
}


def read_arch(tbl):
    with open(ARCH / f"{tbl}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


# columns needing a normalizing expression instead of a plain safe_cast
# (zero-pad state FIPS to 2 digits so it matches br_bd_diretorios_us.state.id_state)


def _pad_census_io(col):
    # ATUS stores census industry/occupation codes without leading zeros
    # ("170") plus non-numeric sentinels ("-1"). Restore the canonical
    # 4-digit zero-padded form so the codes match the census_industry_* /
    # census_occupation_* directories; leave sentinels untouched.
    s = f"safe_cast({col} as string)"
    return f"case when regexp_contains({s}, r'^[0-9]+$') then lpad({s}, 4, '0') else {s} end"


CAST_EXPR = {
    "gestfips": "lpad(safe_cast(gestfips as string), 2, '0')",
    **{
        c: _pad_census_io(c)
        for c in [
            "teio1icd",
            "teio1ocd",
            "peio1icd",
            "peio1ocd",
            "peio2icd",
            "peio2ocd",
        ]
    },
}


def col_cast(a):
    name = a["name"]
    if name in CAST_EXPR:
        return f"    {CAST_EXPR[name]} {name}"
    return f"    safe_cast({name} as {CAST[a['bigquery_type']]}) {name}"


def build_sql(tbl):
    arch = read_arch(tbl)
    casts = ",\n".join(col_cast(a) for a in arch)
    return f"""{{{{
    config(
        schema="{DATASET}",
        alias="{tbl}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": 2003, "end": 2029, "interval": 1}},
        }},
        cluster_by=["tucaseid"],
    )
}}}}


select
{casts}
from
    {{{{ set_datalake_project("{DATASET}_staging.{tbl}") }}}}
    as t
"""


def yaml_desc(text, indent):
    pad = " " * indent
    return f"{pad}description: >-\n{pad}  {text}\n"


def build_model_yaml(tbl):
    desc, key = META[tbl]
    arch = read_arch(tbl)
    out = []
    out.append(f"  - name: {DATASET}__{tbl}\n")
    out.append(yaml_desc(desc, 4))
    out.append("    tests:\n")
    out.append("      - dbt_utils.unique_combination_of_columns:\n")
    out.append(f"          combination_of_columns: [{', '.join(key)}]\n")
    out.append("      - not_null_proportion_multiple_columns:\n")
    out.append("          at_least: 0.05\n")
    out.append("    columns:\n")
    for a in arch:
        nm = a["name"]
        out.append(f"      - name: {nm}\n")
        out.append(yaml_desc(a["description"].replace("\n", " "), 8))
        tests = []
        if nm in key:
            tests.append("not_null")
        col_tests = []
        if nm in DIR_REL:
            model, field, custom = DIR_REL[nm]
            testname = "custom_relationships" if custom else "relationships"
            col_tests.append(
                f"          - {testname}:\n"
                f"              to: ref('{model}')\n"
                f"              field: {field}\n"
            )
        if tests or col_tests:
            out.append("        tests:\n")
            for t in tests:
                out.append(f"          - {t}\n")
            out.extend(col_tests)
    return "".join(out)


def main():
    # SQL models
    for tbl in META:
        (MODELS / f"{DATASET}__{tbl}.sql").write_text(build_sql(tbl))
    # schema.yml
    parts = ["---\nversion: 2\nmodels:\n"]
    for tbl in META:
        parts.append(build_model_yaml(tbl))
    (MODELS / "schema.yml").write_text("".join(parts))
    print(
        f"Wrote {len(META)} .sql models + schema.yml to {MODELS.relative_to(ROOT.parent)}"
    )


if __name__ == "__main__":
    main()
