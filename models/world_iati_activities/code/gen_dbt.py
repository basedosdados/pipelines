"""Write the dbt models and schema.yml for world_iati_activities.

Everything is derived from the architecture CSVs, so a column added there
appears in the model, in its cast, and in schema.yml without a second edit.

Run ``python gen_dbt.py``, then ``uv run pre-commit run --files
models/world_iati_activities/*`` — sqlfmt and yamlfix rewrite the output, and
committing without that first produces the hook re-write loop.
"""

import csv
import json

from common import ARCH_DIR, OUTPUT, REPO_ROOT
from tables import TABLES

DATASET = "world_iati_activities"
MODEL_DIR = REPO_ROOT / "models" / DATASET

CAST = {
    "STRING": "string",
    "INT64": "int64",
    "FLOAT64": "float64",
    "DATE": "date",
    "DATETIME": "datetime",
    "BOOLEAN": "bool",
}

# Tables partitioned on `year`, and the logical key used for the uniqueness
# test. `_link` is unique within a run for every table that has one, which is
# what the test asserts; transaction_breakdown has no `_link`, so its key is the
# combination the source guarantees.
PARTITIONED = {
    "transaction",
    "transaction_breakdown",
    "budget",
    "planned_disbursement",
    "result_indicator_period",
}

UNIQUE_KEY = {
    "registry_dataset": ["registry_dataset_id"],
    "activity": ["activity_id"],
    "transaction": ["year", "transaction_id"],
    # No unique key exists in the source. A publisher can declare the same
    # sector or the same recipient twice on one activity, and IATI Tables then
    # emits two splits of the same transaction over the identical
    # (sector, country, region) triple with different values: 206,827 such
    # groups covering 441,215 rows, 1.79% of the table. Adding `value` still
    # leaves 47,848. Rather than assert a key that is not there, this table
    # carries no uniqueness test — see the model description.
    "transaction_breakdown": None,
    "transaction_sector": ["transaction_sector_id"],
    "budget": ["year", "budget_id"],
    "planned_disbursement": ["year", "planned_disbursement_id"],
    "sector": ["activity_sector_id"],
    "recipient_country": ["activity_recipient_country_id"],
    "recipient_region": ["activity_recipient_region_id"],
    "participating_org": ["participating_org_id"],
    "related_activity": ["related_activity_id"],
    "policy_marker": ["policy_marker_id"],
    "document_link": ["document_link_id"],
    "location": ["location_id"],
    "result": ["result_id"],
    "result_indicator": ["result_indicator_id"],
    "result_indicator_period": ["year", "result_indicator_period_id"],
    "organisation": ["organisation_id"],
}

# Foreign keys inside this dataset. Directory columns in the architecture point
# at these; the relationships tests below are what actually enforce them.
FK = {
    "registry_dataset_id": "registry_dataset",
    "activity_id": "activity",
    "transaction_id": "transaction",
    "result_id": "result",
    "result_indicator_id": "result_indicator",
}

# Columns that are legitimately below the 5% non-null floor the proportion test
# enforces, with their measured non-null share of the source table. These are
# optional IATI elements almost nobody publishes, not a mapping mistake — the
# empty-column check in verify_parquet.py is what would catch that.
IGNORE_SPARSE = {
    "activity": [
        "budget_not_provided_code",  # 1.58%
        "budget_not_provided_name",  # 1.58%
        "crs_channel_code",  # 0.67%
    ],
    "transaction": [
        "recipient_region_name",  # 4.79%
        "recipient_region_vocabulary_code",  # 0.40%
        "recipient_region_vocabulary_name",  # 0.40%
    ],
    "transaction_sector": ["vocabulary_uri"],  # 0.01%
    "planned_disbursement": ["receiver_org_type_name"],  # 3.03%
    "policy_marker": ["vocabulary_uri"],  # 2.08%
    "document_link": ["description"],  # 3.31%
    "result_indicator_period": [
        "target_value",  # 0.02%
        "actual_value",  # 0.34%
    ],
}


# Table descriptions live in tables.py, shared with register_metadata.py, so
# the dbt description and the backend description cannot drift apart.
DESCRIPTION = {k: v["description_pt"] for k, v in TABLES.items()}


def load(table):
    with (ARCH_DIR / f"sheet_{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def max_year(table):
    """The largest partition actually written, read from the parquet tree."""
    years = [
        int(p.name.split("=")[1])
        for p in (OUTPUT / table).iterdir()
        if p.is_dir() and p.name.startswith("year=")
    ]
    return max(years)


def write_model(table):
    cols = load(table)
    lines = []
    if table in PARTITIONED:
        end = max_year(table) + 5
        partition = (
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": 0, "end": {end}, "interval": 1}},\n'
            "        },\n"
        )
    else:
        partition = ""
    lines.append("{{\n    config(\n")
    lines.append(f'        schema="{DATASET}",\n')
    lines.append(f'        alias="{table}",\n')
    lines.append('        materialized="table",\n')
    lines.append(partition)
    lines.append("    )\n}}\n\n\nselect\n")
    body = [
        f"    safe_cast({c['name']} as {CAST[c['bigquery_type']]}) {c['name']}"
        for c in cols
    ]
    lines.append(",\n".join(body))
    lines.append(
        f'\nfrom\n    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        "    as t\n"
    )
    path = MODEL_DIR / f"{DATASET}__{table}.sql"
    path.write_text("".join(lines), encoding="utf-8")
    return path


def yaml_quote(text):
    return text.replace('"', "'")


def write_schema(tables):
    out = ["---\n", "version: 2\n", "models:\n"]
    for table in tables:
        cols = load(table)
        partitioned = table in PARTITIONED
        # The wide, tall tables get their tests scoped to the most recent
        # partition: the null-proportion test compiles a scan of every column,
        # which is a full-table read on 24.6M rows otherwise.
        scope = (
            "        config:\n          where: __most_recent_year_en__\n"
            if partitioned
            else ""
        )
        out.append(f"  - name: {DATASET}__{table}\n")
        out.append("    description: >\n")
        out.append(f"      {DESCRIPTION[table]}\n")
        out.append("    tests:\n")
        if UNIQUE_KEY[table] is not None:
            out.append("      - dbt_utils.unique_combination_of_columns:\n")
            out.append(
                "          combination_of_columns: "
                f"[{', '.join(UNIQUE_KEY[table])}]\n"
            )
            if scope:
                out.append(scope)
        out.append("      - not_null_proportion_multiple_columns:\n")
        out.append("          at_least: 0.05\n")
        for col in IGNORE_SPARSE.get(table, []):
            if col == IGNORE_SPARSE[table][0]:
                out.append("          ignore_values:\n")
            out.append(f"            - {col}\n")
        if scope:
            out.append(scope)
        out.append("    columns:\n")
        for c in cols:
            out.append(f"      - name: {c['name']}\n")
            out.append(
                f"        description: {yaml_quote(c['description_pt'])}\n"
            )
            tests = []
            key = UNIQUE_KEY[table] or []
            if c["name"] in key or c["name"] == "year":
                tests.append("not_null")
            fk = FK.get(c["name"])
            # A table never points a relationships test at itself.
            if fk and fk != table:
                out.append("        tests:\n")
                for t in tests:
                    out.append(f"          - {t}\n")
                out.append("          - relationships:\n")
                out.append(f"              to: ref('{DATASET}__{fk}')\n")
                out.append(f"              field: {c['name']}\n")
                if partitioned:
                    out.append("              config:\n")
                    out.append(
                        "                where: __most_recent_year_en__\n"
                    )
            elif tests:
                out.append(f"        tests: [{', '.join(tests)}]\n")
    (MODEL_DIR / "schema.yml").write_text("".join(out), encoding="utf-8")


def main():
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    tables = [
        p.name[len("sheet_") : -4]
        for p in sorted(ARCH_DIR.glob("sheet_*.csv"))
    ]
    # registry_dataset first so ref() resolution reads naturally.
    tables.sort(key=lambda t: (t != "registry_dataset", t != "activity", t))
    for table in tables:
        print(write_model(table).name)
    write_schema(tables)
    print("schema.yml")
    print(json.dumps({"tables": len(tables)}))


if __name__ == "__main__":
    main()
