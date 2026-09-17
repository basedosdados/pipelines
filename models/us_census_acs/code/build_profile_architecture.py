#!/usr/bin/env python3
"""Generate architecture CSVs for the 9 profile tables + variables + dicionario.

Profile tables are long/tidy: ano, period, <geo id cols>, variable_code,
estimate, margin_of_error. Geo id columns and directory FKs vary by level.
Descriptions in Portuguese (EN/ES added at metadata step).
"""

import csv
import os

OUT = os.path.join(os.path.dirname(__file__), "architecture")
os.makedirs(OUT, exist_ok=True)
FIELDS = [
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


def row(name, typ, desc, cov="no", directory="", unit="", obs="", orig=""):
    return {
        "name": name,
        "bigquery_type": typ,
        "description": desc,
        "temporal_coverage": "",
        "covered_by_dictionary": cov,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": orig,
    }


ANO = row(
    "year",
    "INT64",
    "Ano de referência do Censo; para estimativas de 5 anos, é o último ano do período de 5 anos",
    directory="br_bd_diretorios_data_tempo.ano:ano",
    obs="Coluna de partição (caminho hive 'ano=' no staging).",
    orig="ano",
)
PERIOD = row(
    "period",
    "STRING",
    "Período da estimativa da ACS: 1-year (1 ano) ou 5-year (5 anos)",
    obs="Valores: 1-year, 5-year. Parte da chave lógica.",
)
VARCODE = row(
    "variable_code",
    "STRING",
    "Código da variável do perfil de dados da ACS (ex.: DP05_0001E), decodificado na tabela variables",
    obs="Relacionamento com a tabela variables. Sufixo E = estimativa; PE = percentual.",
)
EST = row(
    "estimate",
    "FLOAT64",
    "Valor estimado da variável; a unidade de medida varia conforme variable_code (ver variables.unit)",
    obs="Sem unidade de medida a nível de coluna (varia por linha). Sentinelas da ACS convertidas para nulo.",
)
MOE = row(
    "margin_of_error",
    "FLOAT64",
    "Margem de erro (90% de confiança) da estimativa; mesma unidade da estimativa",
    obs="Sem unidade de medida a nível de coluna (varia por linha).",
)

# geo id columns per level: (name, description, directory_column, observations)
STATE = row(
    "id_state",
    "STRING",
    "Código FIPS de 2 dígitos do estado",
    directory="br_bd_diretorios_us.state:id_state",
)


def county():
    return row(
        "id_county",
        "STRING",
        "Código FIPS de 5 dígitos do condado (estado+condado)",
        directory="br_bd_diretorios_us.county:id_county",
    )


def place():
    return row(
        "id_place",
        "STRING",
        "Código GEOID de 7 dígitos do lugar (estado+lugar)",
        directory="br_bd_diretorios_us.place:id_place",
    )


def puma():
    return row(
        "puma",
        "STRING",
        "Código da Public Use Microdata Area (PUMA)",
        obs="Sem FK de diretório: a vintage da PUMA varia por ano (ver D6).",
    )


def cbsa():
    return row(
        "id_cbsa",
        "STRING",
        "Código da Core-Based Statistical Area (área metropolitana/micropolitana)",
        directory="br_bd_diretorios_us.cbsa_2023:id_cbsa",
    )


def cd():
    return row(
        "id_congressional_district",
        "STRING",
        "Código do distrito congressional (estado+distrito)",
        obs="Sem FK de diretório: a vintage segue o Congresso do ano (ver D6b).",
    )


def zcta():
    return row(
        "id_zcta",
        "STRING",
        "Código da Zip Code Tabulation Area (ZCTA5)",
        obs="Sem FK de diretório: a vintage da ZCTA varia por ano (ver D6b).",
    )


def sd():
    return row(
        "id_school_district",
        "STRING",
        "Código GEOID de 7 dígitos do distrito escolar unificado (LEAID)",
        directory="br_bd_diretorios_us.school_district:id_school_district",
    )


def pais():
    return row(
        "id_country",
        "STRING",
        "Código do país (US para Estados Unidos)",
        directory="br_bd_diretorios_mundo.pais:id_pais",
        orig="id_pais",
    )


LEVEL_GEO = {
    "nation": [pais()],
    "state": [STATE],
    "county": [STATE, county()],
    "place": [STATE, place()],
    "puma": [STATE, puma()],
    "cbsa": [cbsa()],
    "congressional_district": [STATE, cd()],
    "zcta": [zcta()],
    "school_district": [STATE, sd()],
}

for level, geo in LEVEL_GEO.items():
    rows = [ANO, PERIOD, *geo, VARCODE, EST, MOE]
    with open(f"{OUT}/data_profile_{level}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

# variables catalog architecture (all STRING)
variables_cols = [
    ("code", "Código da variável do perfil de dados da ACS (ex.: DP05_0001E)"),
    ("profile", "Perfil de dados: DP02, DP03, DP04 ou DP05"),
    (
        "profile_name",
        "Nome do perfil (características sociais, econômicas, habitacionais, demográficas)",
    ),
    ("line_number", "Número da linha da variável dentro do perfil"),
    ("label", "Rótulo hierárquico da variável"),
    ("concept", "Conceito/título da tabela de origem"),
    ("universe", "Universo de referência da variável"),
    (
        "unit",
        "Unidade de medida da variável (count, percent, USD, years, ratio)",
    ),
    ("is_percent", "Indica se a variável é um percentual (yes/no)"),
]
with open(f"{OUT}/variables.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=FIELDS)
    w.writeheader()
    for n, d in variables_cols:
        w.writerow(row(n, "STRING", d, cov=("yes" if n == "code" else "no")))

# dicionario (DB-standard, all STRING)
dic_cols = [
    ("id_tabela", "Identificador da tabela (nome da tabela de dados)"),
    ("nome_coluna", "Nome da coluna coberta pelo dicionário"),
    ("chave", "Valor codificado na coluna original"),
    ("cobertura_temporal", "Cobertura temporal em que a chave é válida"),
    ("valor", "Rótulo correspondente ao valor codificado"),
]
with open(f"{OUT}/dicionario.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=FIELDS)
    w.writeheader()
    for n, d in dic_cols:
        w.writerow(row(n, "STRING", d))

print("wrote architecture CSVs:")
for fn in sorted(os.listdir(OUT)):
    print("  ", fn)
