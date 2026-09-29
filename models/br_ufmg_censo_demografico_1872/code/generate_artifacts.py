"""Generate the architecture tables, dbt models and schema.yml for this dataset.

58 published tables share a handful of shapes, so the artifacts are generated
from ``spec.py`` rather than written out. That keeps column names, types,
descriptions and the dbt casts in lockstep with the transform: a change in the
spec cannot leave the dbt model or the architecture behind.

Writes:
  code/architecture/<table_slug>.csv     architecture table, one per table
  code/columns.json                      PT/EN/ES descriptions for metadata
  ../br_ufmg_censo_demografico_1872__<table_slug>.sql
  ../schema.yml

Run:  uv run python models/br_ufmg_censo_demografico_1872/code/generate_artifacts.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from models.br_ufmg_censo_demografico_1872.code.descriptions import (
    KEY_COLUMNS,
    measure_description,
)
from models.br_ufmg_censo_demografico_1872.code.spec import (
    ANO,
    LEVELS,
    TABLES,
    table_slug,
)
from models.br_ufmg_censo_demografico_1872.code.tables import (
    AUX_TITLES,
    AUXILIARES,
    DATASET_ID,
    STEMS,
    table_description,
    table_title,
)

HERE = Path(__file__).resolve().parent
ARCH_DIR = HERE / "architecture"
MODEL_DIR = HERE.parent

ARCH_HEADER = [
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

# Only `ano` has a Data Basis directory. The 1872 province, municipality and
# parish codes are the Pop-72 codes; no directory covers historical Brazilian
# geography, so linking them would point at codes that do not exist there.
DIRECTORY = {"ano": "br_bd_diretorios_data_tempo.ano:ano"}

# Notes are trilingual: a Portuguese-only note is blank for most of the site's
# readers.
GEO_NOTE = (
    "Código do banco Pop-72, não do IBGE. A fonte não publica tradutor para a "
    "malha municipal atual.",
    "Pop-72 database code, not an IBGE code. The source publishes no crosswalk "
    "to the present-day municipal grid.",
    "Código de la base Pop-72, no del IBGE. La fuente no publica una tabla de "
    "equivalencias con la malla municipal actual.",
)

AGG_NOTE = (
    "Soma dos valores das paróquias que compõem a unidade.",
    "Sum over the parishes that make up the unit.",
    "Suma de los valores de las parroquias que componen la unidad.",
)


def _obs(col: dict, i: int) -> str:
    """One language of a column's trilingual observation note."""
    return (col.get("observations") or ("", "", ""))[i]


def _arch_row(
    name: str,
    bq_type: str,
    description: str,
    *,
    dictionary: bool = False,
    unit: str = "",
    observations: str = "",
    original: str = "",
) -> list[str]:
    return [
        name,
        bq_type,
        description,
        "" if name == "ano" else str(ANO),
        "yes" if dictionary else "no",
        DIRECTORY.get(name, ""),
        unit,
        "no",
        observations,
        original,
    ]


def _columns_for(
    source: str | None, level: str | None, aux: str | None
) -> list[dict]:
    """Ordered column spec for one published table.

    Each entry carries everything both artifacts need: the published name, the
    BigQuery type, the three descriptions, and the source column it came from.
    """
    out: list[dict] = []

    def add(name, bq, pt, en, es, **kw):
        out.append(dict(name=name, bq=bq, pt=pt, en=en, es=es, **kw))

    if aux:
        spec = AUXILIARES[aux]
        if aux != "dicionario":
            add("ano", "INT64", *KEY_COLUMNS["ano"], unit="year")
        for c in spec["cols"]:
            add(c, "STRING", *KEY_COLUMNS[c])
        return out

    assert source is not None and level is not None
    t = TABLES[source]
    add("ano", "INT64", *KEY_COLUMNS["ano"], unit="year")
    for geo in LEVELS[level]:
        add(geo, "STRING", *KEY_COLUMNS[geo], observations=GEO_NOTE)
    if t["categoria_col"]:
        add(
            "id_categoria",
            "STRING",
            *KEY_COLUMNS["id_categoria"],
            dictionary=True,
            original=t["categoria_col"],
        )

    implied = STEMS[t["stem"]]["sexo"]
    unit = STEMS[t["stem"]]["unidade"]
    seen = set()
    for src_col, name in t["measures"].items():
        if name in seen:
            continue
        seen.add(name)
        pt, en, es = measure_description(name, implied)
        note = ("", "", "")
        if level != "paroquia":
            note = AGG_NOTE
        add(
            name,
            "INT64",
            pt,
            en,
            es,
            unit=unit,
            observations=note,
            original=src_col,
        )
    return out


def write_architecture(slug: str, cols: list[dict]) -> None:
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    with (ARCH_DIR / f"{slug}.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(ARCH_HEADER)
        for c in cols:
            w.writerow(
                _arch_row(
                    c["name"],
                    c["bq"],
                    c["pt"],
                    dictionary=c.get("dictionary", False),
                    unit=c.get("unit", ""),
                    observations=_obs(c, 0),
                    original=c.get("original", ""),
                )
            )


def write_model(slug: str, cols: list[dict]) -> None:
    casts = ",\n".join(
        f"    safe_cast({c['name']} as {c['bq'].lower()}) {c['name']}"
        for c in cols
    )
    # `dicionario` carries no `ano`, so it takes no partition: partitioning on a
    # column the table does not have fails with "Unrecognized name: ano".
    partition = ""
    if any(c["name"] == "ano" for c in cols):
        partition = f"""
        partition_by={{
            "field": "ano",
            "data_type": "int64",
            "range": {{"start": {ANO}, "end": {ANO + 5}, "interval": 1}},
        }},"""

    sql = f'''{{{{
    config(
        schema="{DATASET_ID}",
        alias="{slug}",
        materialized="table",{partition}
    )
}}}}


select
{casts}
from
    {{{{ set_datalake_project("{DATASET_ID}_staging.{slug}") }}}}
    as t
'''
    (MODEL_DIR / f"{DATASET_ID}__{slug}.sql").write_text(sql)


def _yaml_block(text: str, indent: int) -> str:
    pad = " " * indent
    return "\n".join(pad + line for line in text.split("\n"))


def write_schema(entries: list[dict]) -> None:
    out = ["---", "version: 2", "models:"]
    for e in entries:
        slug, cols, desc, key = e["slug"], e["cols"], e["pt"], e["key"]
        out.append(f"  - name: {DATASET_ID}__{slug}")
        out.append("    description: >-")
        out.append(_yaml_block(desc, 6))
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(f"          combination_of_columns: [{', '.join(key)}]")
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if any(c.get("dictionary") for c in cols):
            out.append("      - custom_dictionary_coverage:")
            out.append(
                f"          dictionary_model: ref('{DATASET_ID}__dicionario')"
            )
            out.append("          columns_covered_by_dictionary:")
            out.append("            - id_categoria")
        out.append("    columns:")
        for c in cols:
            out.append(f"      - name: {c['name']}")
            out.append("        description: >-")
            out.append(_yaml_block(c["pt"], 10))
            tests = []
            if c["name"] in key or c["name"] == "ano":
                tests.append("not_null")
            if c["name"] == "ano":
                out.append("        tests:")
                out.append("          - not_null")
                out.append("          - relationships:")
                out.append(
                    "              to: ref('br_bd_diretorios_data_tempo__ano')"
                )
                out.append("              field: ano.ano")
                continue
            if tests:
                out.append(f"        tests: [{', '.join(tests)}]")
    (MODEL_DIR / "schema.yml").write_text("\n".join(out) + "\n")


def main() -> None:
    entries = []

    for aux in AUXILIARES:
        cols = _columns_for(None, None, aux)
        spec = AUXILIARES[aux]
        n_pt, n_en, n_es = AUX_TITLES[aux]
        entries.append(
            dict(
                slug=aux,
                cols=cols,
                pt=spec["pt"],
                en=spec["en"],
                es=spec["es"],
                key=spec["key"],
                name_pt=n_pt,
                name_en=n_en,
                name_es=n_es,
            )
        )

    for source, t in TABLES.items():
        for level in LEVELS:
            slug = table_slug(source, level)
            cols = _columns_for(source, level, None)
            pt, en, es = table_description(t["stem"], t["versao"], level)
            key = (
                ["ano"]
                + LEVELS[level]
                + (["id_categoria"] if t["categoria_col"] else [])
            )
            n_pt, n_en, n_es = table_title(t["stem"], t["versao"], level)
            entries.append(
                dict(
                    slug=slug,
                    cols=cols,
                    pt=pt,
                    en=en,
                    es=es,
                    key=key,
                    name_pt=n_pt,
                    name_en=n_en,
                    name_es=n_es,
                )
            )

    for e in entries:
        # pyrefly: ignore [bad-argument-type]
        write_architecture(e["slug"], e["cols"])
        # pyrefly: ignore [bad-argument-type]
        write_model(e["slug"], e["cols"])
    write_schema(entries)

    (HERE / "columns.json").write_text(
        json.dumps(
            {
                e["slug"]: dict(
                    name_pt=e["name_pt"],
                    name_en=e["name_en"],
                    name_es=e["name_es"],
                    description_pt=e["pt"],
                    description_en=e["en"],
                    description_es=e["es"],
                    columns=[
                        dict(
                            name=c["name"],  # pyrefly: ignore [bad-index]
                            bigquery_type=c["bq"],  # pyrefly: ignore [bad-index]
                            description_pt=c["pt"],  # pyrefly: ignore [bad-index]
                            description_en=c["en"],  # pyrefly: ignore [bad-index]
                            description_es=c["es"],  # pyrefly: ignore [bad-index]
                            covered_by_dictionary=bool(c.get("dictionary")),  # pyrefly: ignore [missing-attribute]
                            measurement_unit=c.get("unit", ""),  # pyrefly: ignore [missing-attribute]
                            is_partition=c["name"] == "ano",  # pyrefly: ignore [bad-index]
                            directory_column=DIRECTORY.get(c["name"], ""),  # pyrefly: ignore [bad-index]
                            observations_pt=_obs(c, 0),  # pyrefly: ignore [bad-argument-type]
                            observations_en=_obs(c, 1),  # pyrefly: ignore [bad-argument-type]
                            observations_es=_obs(c, 2),  # pyrefly: ignore [bad-argument-type]
                        )
                        for c in e["cols"]
                    ],
                )
                for e in entries
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n"
    )

    print(
        f"{len(entries)} tables: architecture CSVs, dbt models, schema.yml, columns.json"
    )


if __name__ == "__main__":
    main()
