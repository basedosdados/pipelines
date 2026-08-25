"""
Generate dbt SQL models + schema.yml for br_senado_dados_abertos_administrativos
from architecture_spec.py.

Partition ranges and the not_null_proportion ignore list are derived from the
actual parquet, so run this AFTER the extraction and they will be exact.

  uv run python models/br_senado_dados_abertos_administrativos/code/gen_dbt.py
"""

from __future__ import annotations

import glob
import os

import pandas as pd
import pyarrow.parquet as pq

# pyrefly: ignore [missing-import]
from architecture_spec import DIR_ANO, DIR_MES, DIR_UF, TABLES

HERE = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.dirname(HERE)
DATASET = "br_senado_dados_abertos_administrativos"
DATA_DIR = os.environ.get(
    "SENADO_ADM_DATA",
    os.path.expanduser(f"~/Downloads/{DATASET}_data"),
)
OUTPUT = os.path.join(DATA_DIR, "output")

BQ = {
    "str": "string",
    "int": "int64",
    "float": "float64",
    "date": "date",
    "datetime": "datetime",
}
CUR_YEAR = pd.Timestamp.today().year

# Directory foreign keys, mapped to the dbt model and field they point at.
DIRECTORIES = {
    DIR_ANO: ("br_bd_diretorios_data_tempo__ano", "ano.ano"),
    DIR_MES: ("br_bd_diretorios_data_tempo__mes", "mes.mes"),
    DIR_UF: ("br_bd_diretorios_brasil__uf", "sigla"),
}


def load(slug: str) -> pd.DataFrame | None:
    files = glob.glob(
        os.path.join(OUTPUT, slug, "**", "*.parquet"), recursive=True
    )
    if not files:
        return None
    parts = []
    for path in files:
        frame = pq.read_table(path).to_pandas()
        # Reattach the hive partition value, which lives in the path, not the file.
        for segment in path.split(os.sep):
            if "=" in segment:
                key, _, value = segment.partition("=")
                if key in ("ano", "data_extracao"):
                    frame[key] = value
        parts.append(frame)
    return pd.concat(parts, ignore_index=True)


def partition_range(frame: pd.DataFrame | None) -> tuple[int, int]:
    start = 2008  # earliest year any source exposes (CEAPS)
    if frame is not None and "ano" in frame:
        anos = pd.to_numeric(frame["ano"], errors="coerce").dropna()
        if len(anos):
            start = int(anos.min())
    return start, CUR_YEAR + 5


def ignore_values(
    slug: str, spec: dict, frame: pd.DataFrame | None
) -> list[str]:
    """Columns under 5% non-null, which the proportion test must skip.

    Several columns here are legitimately sparse — `numero_formatado` and
    `unidade_gestora` exist only for contratos, `cargo` and `categoria` only for
    cessões pelo Senado, and most `quadro_pessoal` dimensions apply to a single
    source report.
    """
    if frame is None or not len(frame):
        return []
    out = []
    for col in spec["cols"]:
        name = col[0]
        if name in frame.columns and frame[name].notna().mean() < 0.05:
            out.append(name)
    return out


def gen_sql(slug: str, spec: dict, frame: pd.DataFrame | None) -> str:
    part = spec["partition"]
    if part == "ano":
        start, end = partition_range(frame)
        cfg = (
            "{{\n    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{slug}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            f'            "field": "{part}",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        },\n    )\n}}"
        )
    elif part == "data_extracao":
        cfg = (
            "{{\n    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{slug}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            f'            "field": "{part}",\n'
            '            "data_type": "date",\n'
            '            "granularity": "day",\n'
            "        },\n    )\n}}"
        )
    else:
        cfg = (
            "{{\n    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{slug}",\n'
            '        materialized="table",\n'
            "    )\n}}"
        )

    body = "\n".join(
        f"    safe_cast({col[0]} as {BQ[col[1]]}) {col[0]},"
        for col in spec["cols"]
    ).rstrip(",")
    return (
        f"{cfg}\n\n\nselect\n{body}\n"
        f'from {{{{ set_datalake_project("{DATASET}_staging.{slug}") }}}} as t\n'
    )


def gen_schema_entry(slug: str, spec: dict, frame: pd.DataFrame | None) -> str:
    out = [
        f"  - name: {DATASET}__{slug}",
        "    description: >",
        f"      {spec['desc_pt'].strip()}",
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        f"          combination_of_columns: [{', '.join(spec['unique'])}]",
        "      - not_null_proportion_multiple_columns:",
        "          at_least: 0.05",
    ]
    skip = ignore_values(slug, spec, frame)
    if skip:
        out.append("          ignore_values:")
        out.extend(f"            - {c}" for c in skip)
    out.append("    columns:")
    for col in spec["cols"]:
        name, pt = col[0], col[2]
        opts = col[5] if len(col) > 5 else {}
        out.append(f"      - name: {name}")
        out.append("        description: >")
        out.append(f"          {pt}")
        tests = []
        if opts.get("notnull"):
            tests.append("          - not_null")
        directory = DIRECTORIES.get(opts.get("dir", ""))
        if directory:
            model, field = directory
            tests.append("          - relationships:")
            tests.append(f"              to: ref('{model}')")
            tests.append(f"              field: {field}")
        # Intra-dataset foreign key (e.g. id_senador -> the senador dimension).
        # ref value is the target table slug; the field is this column's name.
        ref = opts.get("ref")
        if ref:
            tests.append("          - relationships:")
            tests.append(f"              to: ref('{DATASET}__{ref}')")
            tests.append(f"              field: {name}")
            tests.append("              config:")
            tests.append(f'                where: "{name} is not null"')
        if tests:
            out.append("        tests:")
            out.extend(tests)
    return "\n".join(out)


def main() -> None:
    frames = {slug: load(slug) for slug in TABLES}
    missing = [s for s, f in frames.items() if f is None]
    if missing:
        print(
            f"note: no parquet for {len(missing)} table(s) — partition ranges "
            f"and ignore_values fall back to defaults: {', '.join(missing)}"
        )

    for slug, spec in TABLES.items():
        path = os.path.join(MODELS_DIR, f"{DATASET}__{slug}.sql")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(gen_sql(slug, spec, frames[slug]))
    print(f"wrote {len(TABLES)} dbt models")

    schema = ["---", "version: 2", "models:"]
    for slug, spec in TABLES.items():
        schema.append(gen_schema_entry(slug, spec, frames[slug]))
    with open(
        os.path.join(MODELS_DIR, "schema.yml"), "w", encoding="utf-8"
    ) as fh:
        fh.write("\n".join(schema) + "\n")
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
