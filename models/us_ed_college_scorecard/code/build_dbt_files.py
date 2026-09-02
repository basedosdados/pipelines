#!/usr/bin/env python3
"""
Generate the dbt models and schema.yml for us_ed_college_scorecard.

Column order, types and descriptions all come from the architecture CSVs in
code/architecture/, which are the single source of truth: if the architecture
and a model disagree, the architecture wins and this script regenerates the
model.

Usage:
    /tmp/cs_venv/bin/python models/us_ed_college_scorecard/code/build_dbt_files.py
"""

import csv
import logging
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]
import spec

DATASET = "us_ed_college_scorecard"
CODE_DIR = pathlib.Path(__file__).resolve().parent
ARCH_DIR = CODE_DIR / "architecture"
MODEL_DIR = CODE_DIR.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dbt")

LONG_TABLES = sorted(set(spec.LONG_TABLES.values()))

# Partition ranges: first cohort year in the archive, last + 5.
YEAR_RANGE = {"institution": (1996, 2030), "field_of_study": (2015, 2027)}
for _t in LONG_TABLES:
    YEAR_RANGE[_t] = (1996, 2030)

UNPARTITIONED = ("variable", "dicionario")

CLUSTER_BY = {t: ["variable_name"] for t in LONG_TABLES}
CLUSTER_BY["field_of_study"] = ["cip_code"]

PRIMARY_KEY = {
    "institution": ["year", "unitid"],
    "field_of_study": ["year", "unitid", "cip_code", "credential_level"],
    "variable": ["variable_name", "source_file"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
    **{t: ["year", "unitid", "variable_name"] for t in LONG_TABLES},
}

TABLE_DESCRIPTION = {
    "institution": (
        "One row per Title IV postsecondary institution and cohort year, carrying the "
        "institution's identity, location, control and degree level, accreditation and "
        "Title IV eligibility, minority-serving designations, undergraduate enrollment, "
        "institutional finance, and the full admissions block (admission rate, SAT and "
        "ACT score distributions). `unitid` is the IPEDS UNITID, so this table joins to "
        "the us_ed_ipeds dataset; that dataset types the key as INT64, so the join needs "
        "safe_cast(unitid as int64). Every other institution-level measure published by "
        "the source is in the long tables of this dataset, one per measurement domain."
    ),
    "academics": (
        "Long table of academic-program measures by institution and cohort year: the share "
        "of degrees awarded in each 2-digit CIP field, the credential levels offered in "
        "each field, and the institution's largest reported programs. One row per "
        "institution, year and source variable."
    ),
    "student_body": (
        "Long table of student-body measures by institution and cohort year: enrollment "
        "by race, gender and attendance status, age, dependency, family income, first- "
        "generation status, and retention rates. One row per institution, year and source "
        "variable."
    ),
    "cost": (
        "Long table of cost measures by institution and cohort year: average net price "
        "overall and by family-income bracket, cost of attendance, and published tuition "
        "and fees. One row per institution, year and source variable."
    ),
    "aid_debt": (
        "Long table of student aid and cumulative debt measures by institution and cohort "
        "year: Pell and federal loan receipt, median debt overall and by student subgroup, "
        "cumulative debt percentiles, and Parent PLUS debt. One row per institution, year "
        "and source variable."
    ),
    "completion": (
        "Long table of completion and outcome measures by institution and cohort year: "
        "completion rates at 100%, 150% and 200% of normal time, disaggregated by race, "
        "Pell receipt, loan status and family income; the eight-year outcome measures by "
        "entry status; transfer rates; and the underlying cohort counts. One row per "
        "institution, year and source variable."
    ),
    "repayment": (
        "Long table of loan repayment measures by institution and cohort year: repayment "
        "rates at 1, 3, 5 and 7 years, cohort default rates, and the borrower-based "
        "repayment status distribution (default, delinquency, forbearance, deferment, "
        "progress, paid in full, discharged). Many borrower-based values are published as "
        "rounding intervals rather than numbers and are therefore in value_raw, not value. "
        "One row per institution, year and source variable."
    ),
    "earnings": (
        "Long table of post-enrollment earnings measures by institution and cohort year: "
        "mean and median earnings and earnings percentiles at 6, 7, 8, 9, 10 and 11 years "
        "after entry, the share earning above a high-school-graduate threshold, and "
        "earnings disaggregated by family income, gender and completion status. One row "
        "per institution, year and source variable."
    ),
    "field_of_study": (
        "One row per institution, 4-digit CIP field of study, credential level and cohort "
        "year, carrying median and mean cumulative debt by student subgroup, estimated "
        "monthly loan payment, median earnings at 1 to 5 years after completion by "
        "subgroup, and borrower-based repayment status. This is the table that pairs "
        "earnings with major. Borrower-based repayment columns (BBRR*) are STRING because "
        "the source publishes them as rounding intervals as often as numbers."
    ),
    "variable": (
        "Catalogue of every variable published in the College Scorecard institution-level "
        "and field-of-study files, with the table of this dataset it was loaded into, its "
        "name in the source's API hierarchy, its declared type, and its definition from "
        "the official data dictionary. Resolves the variable_name column of the long "
        "tables."
    ),
    "dicionario": (
        "Value labels for the coded columns of the institution and field_of_study tables, "
        "as published in the official data dictionary workbook."
    ),
}

SUPPRESSED_NOTE = (
    "Suppression: the source withholds small cells as 'PrivacySuppressed'. In the long "
    "tables such a cell is kept as a row with value null and value_raw "
    "'PrivacySuppressed', so a withheld value stays distinguishable from one that was "
    "never collected; in the two wide tables it becomes null."
)


def read_arch(table):
    with open(ARCH_DIR / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def read_i18n():
    out = {}
    with open(ARCH_DIR / "i18n.csv", newline="") as fh:
        for r in csv.DictReader(fh):
            out[(r["table"], r["name"])] = r
    return out


def cast(name, bq_type):
    if bq_type == "GEOGRAPHY":
        return f"st_geogfromtext(safe_cast({name} as string), make_valid => true) {name}"
    return f"safe_cast({name} as {bq_type.lower()}) {name}"


def build_sql(table, rows):
    cfg = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table not in UNPARTITIONED:
        start, end = YEAR_RANGE[table]
        cfg.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        },"
        )
    if table in CLUSTER_BY:
        cfg.append(f"        cluster_by={CLUSTER_BY[table]!r},")
    selects = ",\n".join(
        "    " + cast(r["name"], r["bigquery_type"]) for r in rows
    )
    return (
        "{{\n    config(\n" + "\n".join(cfg) + "\n    )\n}}\n\n\n"
        "select\n" + selects + "\nfrom\n"
        f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        "    as t\n"
    )


def yaml_block(text, indent):
    pad = " " * indent
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 78:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    return "\n".join(pad + ln for ln in lines)


def build_schema(tables, arch, i18n_map):
    out = ["---", "version: 2", "models:"]
    for table in tables:
        rows = arch[table]
        desc = TABLE_DESCRIPTION[table]
        if table in LONG_TABLES or table in ("institution", "field_of_study"):
            desc = f"{desc} {SUPPRESSED_NOTE}"
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >-")
        out.append(yaml_block(desc, 6))
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(f"          combination_of_columns: {PRIMARY_KEY[table]}")
        # `value` and `value_raw` are null by construction on complementary
        # rows, so they are excluded from the null-proportion floor.
        ignore = [
            r["name"] for r in rows if r["name"] in ("value", "value_raw")
        ]
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if ignore:
            out.append("          ignore_values:")
            out += [f"            - {c}" for c in ignore]
        out.append("    columns:")
        for r in rows:
            name = r["name"]
            out.append(f"      - name: {name}")
            out.append("        description: >-")
            out.append(
                yaml_block(i18n_map[(table, name)]["description_en"], 10)
            )
            tests = []
            if name in PRIMARY_KEY[table]:
                tests.append("not_null")
            if name == "year" and table not in UNPARTITIONED:
                out.append("        tests:")
                out.append("          - not_null")
                out.append("          - relationships:")
                out.append(
                    "              to: ref('br_bd_diretorios_data_tempo__ano')"
                )
                out.append("              field: ano.ano")
                continue
            if name == "variable_name" and table in LONG_TABLES:
                out.append("        tests:")
                out.append("          - not_null")
                out.append("          - relationships:")
                out.append(f"              to: ref('{DATASET}__variable')")
                out.append("              field: variable_name")
                continue
            if tests:
                out.append(f"        tests: [{', '.join(tests)}]")
    return "\n".join(out) + "\n"


def main():
    tables = spec.TABLE_SLUGS
    arch = {t: read_arch(t) for t in tables}
    i18n_map = read_i18n()
    for table in tables:
        path = MODEL_DIR / f"{DATASET}__{table}.sql"
        path.write_text(build_sql(table, arch[table]))
        log.info("%-40s %3d columns", path.name, len(arch[table]))
    (MODEL_DIR / "schema.yml").write_text(build_schema(tables, arch, i18n_map))
    log.info("schema.yml written for %d models", len(tables))


if __name__ == "__main__":
    main()
