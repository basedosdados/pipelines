"""Generate the us_fhfa_hpi dbt models and schema.yml from the architecture CSVs.

Column order, types and English column descriptions all come from
`architecture/*.csv`, so the models cannot drift from the architecture.

Run:  uv run python models/us_fhfa_hpi/code/build_dbt.py
"""

import textwrap
from pathlib import Path

from pipelines.datasets.us_fhfa_hpi.constants import constants
from pipelines.datasets.us_fhfa_hpi.utils import read_arch

MODEL_DIR = Path(__file__).resolve().parent.parent
DATASET = constants.DATASET_ID.value

# First year present in each table, verified against the cleaned parquet output.
# The partition range ends five years past the latest published period.
YEAR_RANGE = {
    "monthly_national": (1991, 2031),
    "quarterly_national": (1975, 2031),
    "quarterly_state": (1975, 2031),
    "quarterly_metro": (1975, 2031),
    "annual_national": (1975, 2030),
    "annual_state": (1975, 2030),
    "annual_cbsa": (1975, 2030),
    "annual_county": (1975, 2030),
    "annual_zip3": (1975, 2030),
    "annual_zip5": (1975, 2030),
    "annual_tract": (1975, 2030),
}

# Clustering key for the tables large enough to benefit from one.
CLUSTER = {
    "quarterly_metro": ["cbsa_id"],
    "annual_county": ["county_id"],
    "annual_zip5": ["zip_code_5"],
    "annual_tract": ["census_tract_id"],
}

# Uniqueness key per table — verified against the cleaned output (0 duplicates).
KEYS = {
    "monthly_national": [
        "year",
        "month",
        "place_id",
        "index_type",
        "index_flavor",
    ],
    "quarterly_national": [
        "year",
        "quarter",
        "place_id",
        "index_type",
        "index_flavor",
    ],
    "quarterly_state": [
        "year",
        "quarter",
        "state_abbreviation",
        "index_type",
        "index_flavor",
    ],
    "quarterly_metro": [
        "year",
        "quarter",
        "cbsa_id",
        "index_type",
        "index_flavor",
    ],
    "annual_national": ["year"],
    "annual_state": ["year", "state_id"],
    "annual_cbsa": ["year", "cbsa_id"],
    "annual_county": ["year", "county_id"],
    "annual_zip3": ["year", "zip_code_3"],
    "annual_zip5": ["year", "zip_code_5"],
    "annual_tract": ["year", "census_tract_id"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

# Columns whose non-null share sits below the 0.05 floor of the proportion test.
IGNORE_NULL = {
    "quarterly_metro": ["note"],
    "dicionario": ["cobertura_temporal"],
}

# Directory foreign keys, as (model, field) per column.
RELATIONSHIPS = {
    "year": ("br_bd_diretorios_data_tempo__ano", "ano.ano"),
    "month": ("br_bd_diretorios_data_tempo__mes", "mes.mes"),
    "state_abbreviation": ("br_bd_diretorios_us__state", "abbreviation"),
    "state_id": ("br_bd_diretorios_us__state", "id_state"),
    "county_id": ("br_bd_diretorios_us__county", "id_county"),
}

DICTIONARY_TABLES = [
    "monthly_national",
    "quarterly_national",
    "quarterly_state",
    "quarterly_metro",
]

DESCRIPTIONS_PT = {
    "monthly_national": (
        "Índice de preços de imóveis (HPI) da FHFA em frequência mensal para os Estados Unidos "
        "e as nove divisões censitárias, estimado apenas com preços de compra. Uma linha por "
        "lugar, ano, mês e variante do índice, com os valores com e sem ajuste sazonal. "
        "Cobertura de janeiro de 1991 em diante."
    ),
    "quarterly_national": (
        "Índice de preços de imóveis (HPI) da FHFA em frequência trimestral para os Estados "
        "Unidos e as nove divisões censitárias. Uma linha por lugar, ano, trimestre e variante "
        "do índice. Apenas a série all-transactions começa antes de 1991."
    ),
    "quarterly_state": (
        "Índice de preços de imóveis (HPI) da FHFA em frequência trimestral para os 50 estados, "
        "o Distrito de Columbia e Porto Rico. Uma linha por estado, ano, trimestre e variante do "
        "índice. Inclui a série non-metro, que cobre a porção não metropolitana do estado."
    ),
    "quarterly_metro": (
        "Índice de preços de imóveis (HPI) da FHFA em frequência trimestral para as áreas "
        "estatísticas metropolitanas e divisões metropolitanas. Uma linha por área, ano, "
        "trimestre e variante do índice, com o erro padrão relativo das séries expanded-data."
    ),
    "annual_national": (
        "Índice anual de preços de imóveis dos Estados Unidos (índice em desenvolvimento da "
        "FHFA, all-transactions, sem ajuste sazonal), de 1975 em diante. Uma linha por ano."
    ),
    "annual_state": (
        "Índice anual de preços de imóveis por estado (índice em desenvolvimento da FHFA, "
        "all-transactions, sem ajuste sazonal), de 1975 em diante. Uma linha por estado e ano."
    ),
    "annual_cbsa": (
        "Índice anual de preços de imóveis por Core Based Statistical Area (índice em "
        "desenvolvimento da FHFA, all-transactions, sem ajuste sazonal), de 1975 em diante. "
        "Inclui o resíduo não CBSA de cada estado, identificado pelo código FIPS estadual."
    ),
    "annual_county": (
        "Índice anual de preços de imóveis por condado (índice em desenvolvimento da FHFA, "
        "all-transactions, sem ajuste sazonal), de 1975 em diante. Uma linha por condado e ano."
    ),
    "annual_zip3": (
        "Índice anual de preços de imóveis por prefixo de três dígitos do ZIP Code (índice em "
        "desenvolvimento da FHFA, all-transactions, sem ajuste sazonal), de 1975 em diante."
    ),
    "annual_zip5": (
        "Índice anual de preços de imóveis por ZIP Code de cinco dígitos (índice em "
        "desenvolvimento da FHFA, all-transactions, sem ajuste sazonal), de 1975 em diante."
    ),
    "annual_tract": (
        "Índice anual de preços de imóveis por setor censitário (índice em desenvolvimento da "
        "FHFA, all-transactions, sem ajuste sazonal), de 1975 em diante. A FHFA publica a linha "
        "mesmo quando suprime o valor do índice por número insuficiente de revendas."
    ),
    "dicionario": (
        "Dicionário de códigos das colunas categóricas do conjunto us_fhfa_hpi: variante do "
        "índice (index_type) e base de dados de origem (index_flavor)."
    ),
}


def sql_for(table: str) -> str:
    arch = read_arch(table)
    cfg = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table in YEAR_RANGE:
        start, end = YEAR_RANGE[table]
        cfg.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        },"
        )
    if table in CLUSTER:
        cols = ", ".join(f'"{c}"' for c in CLUSTER[table])
        cfg.append(f"        cluster_by=[{cols}],")
    selects = ",\n".join(
        f"    safe_cast({a['name']} as {a['bigquery_type'].lower()}) {a['name']}"
        for a in arch
    )
    return (
        "{{\n    config(\n"
        + "\n".join(cfg)
        + "\n    )\n}}\n\n\nselect\n"
        + selects
        + f'\nfrom {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def _block(text: str, indent: str) -> str:
    wrapped = textwrap.wrap(text, width=92 - len(indent))
    return "\n".join(indent + line for line in wrapped)


def schema_for(table: str) -> str:
    arch = read_arch(table)
    out = [
        f"  - name: {DATASET}__{table}",
        "    description: >-",
        _block(DESCRIPTIONS_PT[table], "      "),
        "    tests:",
    ]
    out.append("      - dbt_utils.unique_combination_of_columns:")
    out.append("          combination_of_columns:")
    out += [f"            - {c}" for c in KEYS[table]]
    out.append("      - not_null_proportion_multiple_columns:")
    out.append("          at_least: 0.05")
    if table in IGNORE_NULL:
        out.append("          ignore_values:")
        out += [f"            - {c}" for c in IGNORE_NULL[table]]
    if table in DICTIONARY_TABLES:
        out.append("      - custom_dictionary_coverage:")
        out.append(
            "          columns_covered_by_dictionary: [index_type, index_flavor]"
        )
        out.append(f"          dictionary_model: ref('{DATASET}__dicionario')")
    out.append("    columns:")
    for a in arch:
        out.append(f"      - name: {a['name']}")
        out.append("        description: >-")
        out.append(_block(a["description"], "          "))
        tests = []
        if a["name"] in KEYS[table]:
            tests.append("not_null")
        rel = RELATIONSHIPS.get(a["name"]) if a["directory_column"] else None
        if tests and not rel:
            out.append(f"        tests: [{', '.join(tests)}]")
        elif rel:
            out.append("        tests:")
            for t in tests:
                out.append(f"          - {t}")
            out.append("          - relationships:")
            out.append(f"              to: ref('{rel[0]}')")
            out.append(f"              field: {rel[1]}")
    return "\n".join(out)


def main() -> None:
    tables = constants.TABLES.value
    for table in tables:
        (MODEL_DIR / f"{DATASET}__{table}.sql").write_text(
            sql_for(table), encoding="utf-8"
        )
    schema = (
        "---\nversion: 2\nmodels:\n"
        + "\n".join(schema_for(t) for t in tables)
        + "\n"
    )
    (MODEL_DIR / "schema.yml").write_text(schema, encoding="utf-8")
    print(f"wrote {len(tables)} models + schema.yml")


if __name__ == "__main__":
    main()
