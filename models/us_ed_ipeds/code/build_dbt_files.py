"""Generate DBT model files (.sql) and schema.yml for us_ed_ipeds.

Usage:
    uv run python models/us_ed_ipeds/code/build_dbt_files.py profile [--cache PATH]
    uv run python models/us_ed_ipeds/code/build_dbt_files.py generate [--cache PATH]

`profile` scans the local partitioned parquet in models/us_ed_ipeds/output/
one table at a time to (1) verify the primary key empirically and (2) compute
per-column null rates. Results are cached to a JSON file.

`generate` reads the architecture CSVs (source of truth for column order,
types and descriptions) plus the profile cache and writes the .sql models and
schema.yml.
"""

import argparse
import csv
import json
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml

CODE_DIR = Path(__file__).resolve().parent
DATASET_DIR = CODE_DIR.parent
ARCH_DIR = CODE_DIR / "architecture"
OUTPUT_DIR = DATASET_DIR / "output"
DATASET_ID = "us_ed_ipeds"

# Earliest year with data per table (partition range start).
MIN_YEARS = {
    "hd": 2002,
    "ic": 2000,
    "ic_ay": 2000,
    "ic_py": 2000,
    "adm": 2014,
    "efia": 2002,
    "effy": 2002,
    "ef_a": 2000,
    "ef_b": 2000,
    "ef_c": 2000,
    "ef_d": 2000,
    "c_a": 2000,
    "sfa": 2002,
    "gr": 1997,
    "gr200": 2008,
    "om": 2015,
    "eap": 2001,
    "sal_is": 2012,
    "al": 2014,
    "f1a": 2002,
    "f2": 2001,
    "f3": 2001,
    "flags": 2004,
}

# Primary key candidates per table, in priority order. `year` is implicit
# (files are hive-partitioned by year), so uniqueness is checked within each
# year file for the remaining columns.
#
# Notes from empirical profiling (duplicate counts in the profile cache):
# - effy: [unitid, effylev] and [unitid, effyalev] alone are NOT unique;
#   the full combination below is.
# - gr: grtype/chrtstat are null for a few early-year rows; `line` is needed
#   to distinguish one row pair in the 1998 file.
# - eap: the survey was redesigned. 2001 keys on typecd/functcd, 2002-2011
#   on eaprectp/ftpt, 2012+ on eapcat. The union of the era-specific key
#   columns is unique in every year.
PK_CANDIDATES = {
    "ef_a": [
        ["unitid", "efalevel"],
        ["unitid", "line", "section"],
        ["unitid", "efalevel", "line", "section"],
    ],
    "ef_b": [
        ["unitid", "efbage", "lstudy"],
        ["unitid", "efbage", "line"],
        ["unitid", "efbage", "line", "lstudy"],
    ],
    "ef_c": [
        ["unitid", "efcstate"],
        ["unitid", "line"],
        ["unitid", "efcstate", "line"],
    ],
    "effy": [
        ["unitid", "effyalev", "effylev", "lstudy"],
    ],
    "c_a": [
        ["unitid", "cipcode", "major_number", "award_level"],
    ],
    "gr": [
        ["unitid", "grtype", "chrtstat", "section", "cohort", "line"],
    ],
    "eap": [
        ["unitid", "eapcat", "eaprectp", "ftpt", "typecd", "functcd"],
    ],
    "om": [
        ["unitid", "omchrt"],
    ],
    "sal_is": [
        ["unitid", "arank"],
    ],
}

# Tables whose intended key fails because of a known upstream cleaning bug
# (reported, pending re-clean). Keep the strict test so the breakage stays
# visible instead of being masked by a relaxed test.
STRICT_EXPECTED_FAIL = {"ic_py": ["unitid"]}

# Tables too wide for the not_null_proportion_multiple_columns test: the
# macro emits one aggregate plus one UNION ALL branch per column, and
# BigQuery fails query planning with "query is too complex" above ~450
# columns (f1a 520, f2 476, sfa 712). The 5% non-null rule was verified
# locally against the partitioned parquet instead (see `profile`): every
# column outside the >95%-null set passes.
SKIP_PROP_TEST = {"f1a", "f2", "sfa"}
# All remaining tables are institution-level.
for _t in MIN_YEARS:
    PK_CANDIDATES.setdefault(_t, [["unitid"]])

MODEL_DESCRIPTIONS = {
    "hd": "Directory information (HD survey component) for all Title IV postsecondary institutions in the United States, one row per institution per year.",
    "ic": "Institutional characteristics (IC survey component) covering educational offerings, organization, admissions, services, and athletic associations, one row per institution per year.",
    "ic_ay": "Student charges for academic-year programs (IC_AY survey component), including tuition, fees, and cost of attendance, one row per institution per year.",
    "ic_py": "Student charges for program-year (vocational) institutions (IC_PY survey component), one row per institution per year.",
    "adm": "Admissions and test scores (ADM survey component), including applications, admissions, enrollments, and SAT/ACT score distributions, one row per institution per year.",
    "efia": "Twelve-month instructional activity (EFIA section of the 12-month enrollment component), credit and contact hours with derived full-time equivalent enrollment, one row per institution per year.",
    "effy": "Twelve-month unduplicated headcount enrollment (EFFY survey component) by race/ethnicity and gender, one row per institution, student level, and year.",
    "ef_a": "Fall enrollment by race/ethnicity, gender, attendance status, and student level (EF_A section of the fall enrollment component), one row per institution, enrollment level category, and year.",
    "ef_b": "Fall enrollment by age category, gender, attendance status, and student level (EF_B section of the fall enrollment component), one row per institution, age category, level of study, and year.",
    "ef_c": "Residence and migration of first-time first-year students (EF_C section of the fall enrollment component), one row per institution, state of residence, and year.",
    "ef_d": "Total entering class, retention rates, and student-to-faculty ratio (EF_D section of the fall enrollment component), one row per institution per year.",
    "c_a": "Completions (C_A survey component): awards conferred by CIP program code, award level, race/ethnicity, and gender, one row per institution, CIP code, first/second major, award level, and year.",
    "sfa": "Student financial aid (SFA survey component): counts and amounts of federal, state, local, and institutional grants and loans, and average net price, one row per institution per year.",
    "gr": "Graduation rates (GR survey component) for cohorts tracked at 150 percent of normal time, by race/ethnicity and gender, one row per institution, cohort type, completion status, and year.",
    "gr200": "Graduation rates at 200 percent of normal time (GR200 survey component), one row per institution per year.",
    "om": "Outcome measures (OM survey component): award and enrollment outcomes of degree/certificate-seeking undergraduate cohorts at four, six, and eight years after entry, one row per institution per year.",
    "eap": "Employees by assigned position (EAP survey component): headcounts by occupational category and faculty status, one row per institution, position category, and year.",
    "sal_is": "Instructional staff salaries (SAL_IS section of the human resources component) by academic rank and gender, one row per institution, academic rank, and year.",
    "al": "Academic libraries (AL survey component): collections, circulation, and library expenditures, one row per institution per year.",
    "f1a": "Finance (F1A survey component) for public institutions using GASB accounting standards: revenues, expenses, assets, and liabilities, one row per institution per fiscal year.",
    "f2": "Finance (F2 survey component) for private not-for-profit institutions using FASB accounting standards, one row per institution per fiscal year.",
    "f3": "Finance (F3 survey component) for private for-profit institutions, one row per institution per fiscal year.",
    "flags": "Response status flags for all IPEDS survey components, indicating response, imputation, and parent/child reporting status, one row per institution per year.",
}

SQL_TYPES = {
    "INT64": "int64",
    "FLOAT64": "float64",
    "STRING": "string",
    "BOOL": "bool",
}


def read_architecture(table):
    with open(ARCH_DIR / f"{table}.csv", newline="", encoding="utf-8") as f:
        return [row for row in csv.DictReader(f) if row["name"].strip()]


def year_files(table):
    return sorted((OUTPUT_DIR / table).glob("year=*/data.parquet"))


# ---------------------------------------------------------------- profiling


def check_pk(table):
    """Return (chosen_key, evidence) — evidence maps candidate -> dup rows."""
    files = year_files(table)
    schema_names = set(pq.read_schema(files[0]).names)
    evidence = {}
    chosen = None
    for cand in PK_CANDIDATES[table]:
        if not set(cand) <= schema_names:
            evidence[",".join(cand)] = "missing columns"
            continue
        dups = 0
        total = 0
        for f in files:
            t = pq.read_table(f, columns=cand)
            total += t.num_rows
            n_unique = t.group_by(cand).aggregate([]).num_rows
            dups += t.num_rows - n_unique
        evidence[",".join(cand)] = {
            "duplicate_rows": dups,
            "total_rows": total,
        }
        if dups == 0:
            chosen = cand
            break
    return chosen, evidence


def null_rates(table):
    """Per-column null proportion across all year files, via parquet stats."""
    files = year_files(table)
    null_counts = {}
    total_rows = 0
    for f in files:
        pf = pq.ParquetFile(f)
        md = pf.metadata
        total_rows += md.num_rows
        names = pf.schema_arrow.names
        file_nulls = {n: 0 for n in names}
        missing_stats = set()
        for rg in range(md.num_row_groups):
            for ci in range(md.num_columns):
                col = md.row_group(rg).column(ci)
                name = col.path_in_schema
                st = col.statistics
                if st is None or not st.has_null_count:
                    missing_stats.add(name)
                else:
                    file_nulls[name] += st.null_count
        if missing_stats:  # fallback: read those columns
            t = pq.read_table(f, columns=sorted(missing_stats))
            for name in missing_stats:
                file_nulls[name] = pc.sum(pc.is_null(t[name])).as_py() or 0
        for n, v in file_nulls.items():
            null_counts[n] = null_counts.get(n, 0) + v
    return {n: v / total_rows for n, v in null_counts.items()}, total_rows


def profile(cache_path):
    results = {}
    if cache_path.exists():
        results = json.loads(cache_path.read_text())
    for table in sorted(MIN_YEARS):  # one table at a time, sequentially
        if table in results:
            print(f"[profile] {table}: cached, skipping")
            continue
        print(f"[profile] {table} ...", flush=True)
        chosen, evidence = check_pk(table)
        rates, total_rows = null_rates(table)
        sparse = sorted(n for n, r in rates.items() if r > 0.95)
        results[table] = {
            "primary_key": chosen,
            "pk_evidence": evidence,
            "total_rows": total_rows,
            "sparse_columns": sparse,
            "null_rates": rates,
        }
        cache_path.write_text(json.dumps(results, indent=1))
        print(
            f"[profile] {table}: pk={chosen} rows={total_rows} "
            f"sparse={len(sparse)}/{len(rates)}"
        )
    return results


# --------------------------------------------------------------- generation


class Folded(str):
    """String rendered as a YAML folded block scalar (>-)."""


def _folded_representer(dumper, data):
    return dumper.represent_scalar(
        "tag:yaml.org,2002:str", str(data), style=">"
    )


yaml.add_representer(Folded, _folded_representer)


def write_sql(table):
    arch = read_architecture(table)
    lines = []
    for row in arch:
        name = row["name"].strip()
        btype = SQL_TYPES[row["bigquery_type"].strip().upper()]
        lines.append(f"    safe_cast({name} as {btype}) {name},")
    lines[-1] = lines[-1].rstrip(",")
    body = "\n".join(lines)
    sql = f"""{{{{
    config(
        schema="{DATASET_ID}",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": {MIN_YEARS[table]}, "end": 2030, "interval": 1}},
        }},
        cluster_by=["unitid"],
    )
}}}}


select
{body}
from
    {{{{ set_datalake_project("{DATASET_ID}_staging.{table}") }}}}
    as t
"""
    path = DATASET_DIR / f"{DATASET_ID}__{table}.sql"
    path.write_text(sql)
    return path


def model_entry(table, prof):
    pk = prof["primary_key"]
    unique_test_note = ""
    if pk is not None:
        unique_test = {
            "dbt_utils.unique_combination_of_columns": {
                "combination_of_columns": ["year", *pk],
            }
        }
    elif table in STRICT_EXPECTED_FAIL:
        # keep the intended strict key: the failure flags a known upstream
        # cleaning bug that must be fixed at the source
        unique_test = {
            "dbt_utils.unique_combination_of_columns": {
                "combination_of_columns": ["year"]
                + STRICT_EXPECTED_FAIL[table],
            }
        }
    else:
        # fall back to the last (most granular) candidate with a relaxed test
        pk = PK_CANDIDATES[table][-1]
        unique_test = {
            "custom_unique_combinations_of_columns": {
                "combination_of_columns": ["year", *pk],
                "proportion_allowed_failures": 0.05,
            }
        }
        unique_test_note = (
            " A small proportion of duplicate keys exists in the raw source,"
            " so the uniqueness test allows up to 5 percent failures."
        )
    tests = [unique_test]
    if table not in SKIP_PROP_TEST:
        prop_test = {"at_least": 0.05}
        if prof["sparse_columns"]:
            prop_test["ignore_values"] = prof["sparse_columns"]
        tests.append({"not_null_proportion_multiple_columns": prop_test})
    entry = {
        "name": f"{DATASET_ID}__{table}",
        "description": Folded(MODEL_DESCRIPTIONS[table] + unique_test_note),
        "tests": tests,
        "columns": [],
    }
    for row in read_architecture(table):
        name = row["name"].strip()
        col = {"name": name, "description": Folded(row["description"].strip())}
        if name == "year":
            col["tests"] = [
                "not_null",
                {
                    "relationships": {
                        "to": "ref('br_bd_diretorios_data_tempo__ano')",
                        "field": "ano.ano",
                    }
                },
            ]
        elif name == "unitid":
            col["tests"] = ["not_null"]
        entry["columns"].append(col)
    return entry


def generate(cache_path):
    profiles = json.loads(cache_path.read_text())
    paths = []
    models = []
    for table in sorted(MIN_YEARS):
        paths.append(write_sql(table))
        models.append(model_entry(table, profiles[table]))
    doc = {"version": 2, "models": models}
    schema_path = DATASET_DIR / "schema.yml"
    with open(schema_path, "w", encoding="utf-8") as f:
        f.write("---\n")
        yaml.dump(doc, f, sort_keys=False, allow_unicode=True, width=88)
    paths.append(schema_path)
    for p in paths:
        print(f"[generate] wrote {p.relative_to(DATASET_DIR.parent.parent)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["profile", "generate"])
    ap.add_argument("--cache", default=str(CODE_DIR / "profile_cache.json"))
    args = ap.parse_args()
    cache_path = Path(args.cache)
    if args.command == "profile":
        profile(cache_path)
    else:
        generate(cache_path)


if __name__ == "__main__":
    main()
