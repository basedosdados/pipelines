#!/usr/bin/env python
"""Generate every downstream artifact from ``architecture_def.py``.

    python models/us_osha_enforcement/code/generate.py

Writes:

* ``models/us_osha_enforcement/code/architecture/<table>.csv`` — the
  architecture tables, in the Data Basis sheet layout.
* ``models/us_osha_enforcement/us_osha_enforcement__<table>.sql`` — one dbt
  model per table.
* ``models/us_osha_enforcement/schema.yml`` — descriptions and tests.
* ``models/us_osha_enforcement/code/columns_json/<table>.json`` — payloads for
  the backend ``bulk_upsert_columns``.

Nothing here is hand-edited: the architecture is the single source of truth, so
the dbt model and the registered metadata cannot disagree with it.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
MODEL_DIR = HERE.parent
DATASET = "us_osha_enforcement"

ARCH_COLUMNS = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

PARTITION_START, PARTITION_END = 1970, 2035

#: Referential tests. Each child column is checked against its parent's key.
#:
#: accident_injury is deliberately absent from both directions. 22,509 of the
#: 188,305 incidents its rows reference have no row in `accident` — the source
#: publishes injury detail for investigations whose incident record it does not
#: release — and 87 rows name an inspection that is likewise unpublished. Both
#: are source gaps, not cleaning defects, so a relationships test there would
#: fail on data that is correct.
FOREIGN_KEYS: dict[str, list[tuple[str, str, str]]] = {
    "violation": [("inspection_id", "inspection", "inspection_id")],
    "violation_event": [("inspection_id", "inspection", "inspection_id")],
    "violation_text": [("inspection_id", "inspection", "inspection_id")],
    "related_activity": [("inspection_id", "inspection", "inspection_id")],
    "emphasis_code": [("inspection_id", "inspection", "inspection_id")],
    "optional_code_info": [("inspection_id", "inspection", "inspection_id")],
    "accident_narrative": [("accident_id", "accident", "accident_id")],
}


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    if spec is None or spec.loader is None:  # pragma: no cover
        raise RuntimeError(f"cannot load {name}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def write_architecture(tables) -> None:
    out = HERE / "architecture"
    out.mkdir(parents=True, exist_ok=True)
    for table in tables:
        with open(
            out / f"{table.slug}.csv", "w", newline="", encoding="utf-8"
        ) as fh:
            w = csv.DictWriter(
                fh, fieldnames=ARCH_COLUMNS, lineterminator="\n"
            )
            w.writeheader()
            for col in table.columns:
                w.writerow({k: getattr(col, k) for k in ARCH_COLUMNS})
    print(f"architecture: {len(tables)} CSVs -> {out}")


def write_columns_json(tables) -> None:
    out = HERE / "columns_json"
    out.mkdir(parents=True, exist_ok=True)
    for table in tables:
        payload = [
            {
                "name": col.name,
                "bigquery_type": col.bigquery_type,
                "description": col.description,
                "description_en": col.description_en,
                "description_es": col.description_es,
                "covered_by_dictionary": col.covered_by_dictionary,
                "directory_column": col.directory_column,
                "measurement_unit": col.measurement_unit,
                "has_sensitive_data": col.has_sensitive_data,
                "observations": col.observations,
                "is_partition": col.name in table.partition,
                "original_name": col.original_name,
            }
            for col in table.columns
        ]
        (out / f"{table.slug}.json").write_text(
            json.dumps(payload, indent=1, ensure_ascii=False), encoding="utf-8"
        )
    print(f"columns_json: {len(tables)} payloads -> {out}")


def _model_sql(table) -> str:
    casts = []
    for col in table.columns:
        t = col.bigquery_type.lower()
        casts.append(f"    safe_cast({col.name} as {t}) {col.name},")
    casts[-1] = casts[-1].rstrip(",")
    if table.partition:
        config = (
            "{{\n    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{table.slug}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {PARTITION_START}, '
            f'"end": {PARTITION_END}, "interval": 1}},\n'
            "        },\n"
        )
        cluster = {
            "inspection": '["site_state", "naics_code"]',
            "violation": '["inspection_id", "violation_type"]',
            "violation_event": '["inspection_id"]',
            "violation_text": '["inspection_id"]',
            "related_activity": '["inspection_id"]',
            "emphasis_code": '["inspection_id"]',
            "optional_code_info": '["inspection_id"]',
            "accident": '["accident_id"]',
            "accident_injury": '["accident_id"]',
            "accident_narrative": '["accident_id"]',
        }.get(table.slug)
        if cluster:
            config += f"        cluster_by={cluster},\n"
        config += "    )\n}}"
    else:
        config = (
            "{{\n    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{table.slug}",\n'
            '        materialized="table",\n'
            "    )\n}}"
        )
    body = "\n".join(casts)
    return (
        f"{config}\n\n\n"
        f"select\n{body}\n"
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table.slug}") }}}} as t\n'
    )


def write_models(tables) -> None:
    for table in tables:
        (MODEL_DIR / f"{DATASET}__{table.slug}.sql").write_text(
            _model_sql(table), encoding="utf-8"
        )
    print(f"dbt models: {len(tables)} -> {MODEL_DIR}")


def _yaml_block(text: str, indent: str) -> str:
    """Fold a description under a ``>-`` scalar.

    ``>-`` and not ``>``: a plain ``>`` appends a trailing newline, which lands
    in the BigQuery column description and then never matches the description
    registered through the API.
    """
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 76:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    if cur:
        lines.append(cur)
    return "\n".join(f"{indent}{ln}" for ln in lines)


def write_schema(tables, sparse: dict[str, list[str]], excluded: set) -> None:
    out = ["---", "version: 2", "models:"]
    for table in tables:
        out.append(f"  - name: {DATASET}__{table.slug}")
        out.append("    description: >-")
        out.append(_yaml_block(table.description_pt, "      "))
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            "          combination_of_columns: ["
            + ", ".join(table.primary_key)
            + "]"
        )
        covered = [
            c.name
            for c in table.columns
            if c.covered_by_dictionary == "yes"
            and (table.slug, c.name) not in excluded
        ]
        if covered and table.slug != "dicionario":
            out.append("      - custom_dictionary_coverage:")
            out.append(
                "          columns_covered_by_dictionary: ["
                + ", ".join(covered)
                + "]"
            )
            out.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
        ignore = sparse.get(table.slug, [])
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if ignore:
            out.append("          ignore_values:")
            out.extend(f"            - {c}" for c in ignore)
        out.append("    columns:")
        for col in table.columns:
            out.append(f"      - name: {col.name}")
            out.append("        description: >-")
            out.append(_yaml_block(col.description, "          "))
            fks = [
                fk
                for fk in FOREIGN_KEYS.get(table.slug, [])
                if fk[0] == col.name
            ]
            simple = []
            if col.name in table.partition or col.name in table.primary_key:
                simple.append("not_null")
            if simple and not fks:
                out.append(f"        tests: [{', '.join(simple)}]")
            elif simple or fks:
                out.append("        tests:")
                out.extend(f"          - {t}" for t in simple)
                for _, parent, field in fks:
                    out.append("          - relationships:")
                    out.append(f"              to: ref('{DATASET}__{parent}')")
                    out.append(f"              field: {field}")
    (MODEL_DIR / "schema.yml").write_text(
        "\n".join(out) + "\n", encoding="utf-8"
    )
    print(f"schema.yml -> {MODEL_DIR / 'schema.yml'}")


def main() -> int:
    arch = _load("architecture_def")
    sparse_path = HERE / "sparse_columns.json"
    sparse = (
        json.loads(sparse_path.read_text()) if sparse_path.exists() else {}
    )
    write_architecture(arch.TABLES)
    write_columns_json(arch.TABLES)
    write_models(arch.TABLES)
    dd = _load("dictionary_def")
    write_schema(arch.TABLES, sparse, dd.DICTIONARY_COVERAGE_EXCLUDED)
    return 0


if __name__ == "__main__":
    sys.exit(main())
