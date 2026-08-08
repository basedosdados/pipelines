"""
Generate dbt SQL models + schema.yml for br_senado_dados_abertos from
architecture_spec.py. Partition ranges and the not_null_proportion ignore list
are derived from the actual parquet (run AFTER the extraction so they are exact).

  uv run python gen_dbt.py
"""

from __future__ import annotations

import glob
import os

import pandas as pd
import pyarrow.parquet as pq

# pyrefly: ignore [missing-import]
from architecture_spec import DIR_ANO, DIR_UF, TABLES

HERE = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.dirname(HERE)  # models/br_senado_dados_abertos
OUTPUT = os.path.join(HERE, "output")
DATASET = "br_senado_dados_abertos"

BQ = {"str": "string", "int": "int64", "date": "date", "datetime": "datetime"}
CUR_YEAR = pd.Timestamp.today().year


def _load(slug: str) -> pd.DataFrame | None:
    files = glob.glob(
        os.path.join(OUTPUT, slug, "**", "*.parquet"), recursive=True
    )
    if not files:
        return None
    parts = []
    for f in files:
        df = pq.read_table(f).to_pandas()
        # reattach ano partition from path for partitioned tables
        for seg in f.split(os.sep):
            if seg.startswith("ano="):
                df["ano"] = seg.split("=", 1)[1]
        parts.append(df)
    return pd.concat(parts, ignore_index=True)


def _partition_range(slug: str, spec: dict) -> tuple[int, int]:
    df = _load(slug)
    start = 1991
    if df is not None and "ano" in df:
        anos = pd.to_numeric(df["ano"], errors="coerce").dropna()
        if len(anos):
            start = int(anos.min())
    return start, CUR_YEAR + 5


def _ignore_values(slug: str, spec: dict) -> list[str]:
    """Columns with < 5% non-null in the full data → excluded from the proportion test."""
    df = _load(slug)
    if df is None or not len(df):
        return []
    ig = []
    for name, *_ in spec["cols"]:
        if name not in df.columns:
            continue
        nonnull = df[name].notna().mean()
        if nonnull < 0.05:
            ig.append(name)
    return ig


def gen_sql(slug: str, spec: dict) -> str:
    part = spec["partition"]
    if part:
        start, end = _partition_range(slug, spec)
        cfg = (
            f"{{{{\n"
            f"    config(\n"
            f'        alias="{slug}",\n'
            f'        schema="{DATASET}",\n'
            f'        materialized="table",\n'
            f"        partition_by={{\n"
            f'            "field": "{part}",\n'
            f'            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            f"        }},\n"
            f"    )\n"
            f"}}}}"
        )
    else:
        cfg = f'{{{{ config(alias="{slug}", schema="{DATASET}") }}}}'

    lines = []
    for col in spec["cols"]:
        name, typ = col[0], col[1]
        opts = col[-1] if isinstance(col[-1], dict) else {}
        src = opts.get("src", name)  # staging column name if it differs
        lines.append(f"    safe_cast({src} as {BQ[typ]}) {name},")
    body = "\n".join(lines)
    return (
        f"{cfg}\n\n"
        f"select\n{body}\n"
        f'from {{{{ set_datalake_project("{DATASET}_staging.{slug}") }}}} as t\n'
    )


def _yaml_desc(desc: str) -> str:
    return "    description: >\n      " + desc.strip()


def gen_schema_entry(slug: str, spec: dict) -> str:
    out = [f"  - name: {DATASET}__{slug}", _yaml_desc(spec["desc_pt"])]
    out.append("    tests:")
    out.append("      - dbt_utils.unique_combination_of_columns:")
    out.append(
        f"          combination_of_columns: [{', '.join(spec['unique'])}]"
    )
    out.append("      - not_null_proportion_multiple_columns:")
    out.append("          at_least: 0.05")
    ig = _ignore_values(slug, spec)
    if ig:
        out.append("          ignore_values:")
        out.extend(f"            - {c}" for c in ig)
    out.append("    columns:")
    for col in spec["cols"]:
        name, pt = col[0], col[2]
        opts = col[-1] if isinstance(col[-1], dict) else {}
        out.append(f"      - name: {name}")
        out.append(f"        description: {pt}")
        tests = []
        if opts.get("notnull"):
            tests.append(("not_null", None, None))
        d = opts.get("dir")
        # dir_except: values legitimately absent from the directory (e.g. extinct
        # UFs) — exempted from the relationship test via a `where` config.
        exc = opts.get("dir_except")
        if d == DIR_ANO:
            tests.append(
                # pyrefly: ignore [bad-argument-type]
                ("rel", ("br_bd_diretorios_data_tempo__ano", "ano.ano"), exc)
            )
        elif d == DIR_UF:
            tests.append(
                # pyrefly: ignore [bad-argument-type]
                ("rel", ("br_bd_diretorios_brasil__uf", "sigla"), exc)
            )
        if tests:
            out.append("        tests:")
            for kind, arg, rel_exc in tests:
                if kind == "not_null":
                    out.append("          - not_null")
                else:
                    # pyrefly: ignore [not-iterable]
                    model, field = arg
                    out.append("          - relationships:")
                    out.append(f"              to: ref('{model}')")
                    out.append(f"              field: {field}")
                    if rel_exc:
                        vals = ", ".join(f"'{v}'" for v in rel_exc)
                        out.append("              config:")
                        out.append(
                            f'                where: "{name} not in ({vals})"'
                        )
    return "\n".join(out)


def main() -> None:
    for slug, spec in TABLES.items():
        path = os.path.join(MODELS_DIR, f"{DATASET}__{slug}.sql")
        with open(path, "w", encoding="utf-8") as f:
            f.write(gen_sql(slug, spec))
        print("wrote", os.path.basename(path))

    schema = ["---", "version: 2", "models:"]
    for slug, spec in TABLES.items():
        schema.append(gen_schema_entry(slug, spec))
    with open(
        os.path.join(MODELS_DIR, "schema.yml"), "w", encoding="utf-8"
    ) as f:
        f.write("\n".join(schema) + "\n")
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
