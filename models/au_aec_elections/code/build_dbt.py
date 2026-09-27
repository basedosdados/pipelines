"""Generate the au_aec_elections dbt models and schema.yml from the architecture.

Run:  uv run python models/au_aec_elections/code/build_dbt.py
"""

from __future__ import annotations

from pipelines.datasets.au_aec_elections import schema
from pipelines.datasets.au_aec_elections.constants import REPO_ROOT, constants

DATASET = constants.DATASET_ID.value
MODELS_DIR = REPO_ROOT / "models" / DATASET

CAST = {
    "STRING": "safe_cast({col} as string) {col},",
    "INT64": "safe_cast({col} as int64) {col},",
    "FLOAT64": "safe_cast({col} as float64) {col},",
    "DATE": "safe_cast({col} as date) {col},",
}

DIRECTORY_REF = {
    "br_bd_diretorios_au.state:abbreviation": (
        "br_bd_diretorios_au__state",
        "abbreviation",
    ),
    # The time directory's `ano` column binds to a STRUCT when referenced bare, so the
    # relationships test must address it as `ano.ano`.
    "br_bd_diretorios_data_tempo.ano:ano": (
        "br_bd_diretorios_data_tempo__ano",
        "ano.ano",
    ),
}


def sql_for(table: str) -> str:
    cols = schema.TABLES[table]
    partition = schema.PARTITION_COLUMNS[table]
    config = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if partition:
        r = schema.PARTITION_RANGE
        config.append(
            "        partition_by={\n"
            f'            "field": "{partition[0]}",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {r["start"]}, "end": {r["end"]}, '
            f'"interval": {r["interval"]}}},\n'
            "        },"
        )
    body = "\n".join(
        "    " + CAST[c.bigquery_type].format(col=c.name) for c in cols
    ).rstrip(",")
    return (
        "{{\n    config(\n"
        + "\n".join(config)
        + "\n    )\n}}\n\n\nselect\n"
        + body
        + "\nfrom\n    {{\n        set_datalake_project(\n"
        + f'            "{DATASET}_staging.{table}"\n'
        + "        )\n    }} as t\n"
    )


def block(text: str, indent: int) -> str:
    pad = " " * indent
    return f"{pad}description: >-\n" + "\n".join(
        f"{pad}  {line}" for line in _wrap(text, 88 - indent)
    )


def _wrap(text: str, width: int) -> list[str]:
    words, lines, cur = text.split(), [], ""
    for w in words:
        if cur and len(cur) + 1 + len(w) > width:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    if cur:
        lines.append(cur)
    return lines


def schema_yml() -> str:
    out = ["---", "version: 2", "models:"]
    for table in constants.TABLES.value:
        meta = schema.TABLE_META[table]
        cols = schema.TABLES[table]
        partition = schema.PARTITION_COLUMNS[table]

        out.append(f"  - name: {DATASET}__{table}")
        out.append(block(meta.description_pt, 4))
        out.append("    tests:")
        if meta.unique_key:
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(
                "          combination_of_columns: ["
                + ", ".join(meta.unique_key)
                + "]"
            )
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if meta.ignore_null_proportion:
            out.append("          ignore_values:")
            for c in meta.ignore_null_proportion:
                out.append(f"            - {c}")
        out.append("    columns:")

        key_cols = (set(meta.unique_key) | set(partition)) - set(
            meta.nullable_key
        )
        for c in cols:
            out.append(f"      - name: {c.name}")
            out.append(block(c.description, 8))
            tests: list[str] = []
            if c.name in key_cols:
                tests.append("not_null")
            ref = DIRECTORY_REF.get(c.directory_column)
            if not tests and not ref:
                continue
            out.append("        tests:")
            for t in tests:
                out.append(f"          - {t}")
            if ref:
                out.append("          - relationships:")
                out.append(f"              to: ref('{ref[0]}')")
                out.append(f"              field: {ref[1]}")
    return "\n".join(out) + "\n"


def main() -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    for table in constants.TABLES.value:
        path = MODELS_DIR / f"{DATASET}__{table}.sql"
        path.write_text(sql_for(table), encoding="utf-8")
        print(f"wrote {path.relative_to(REPO_ROOT)}")
    yml = MODELS_DIR / "schema.yml"
    yml.write_text(schema_yml(), encoding="utf-8")
    print(f"wrote {yml.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
