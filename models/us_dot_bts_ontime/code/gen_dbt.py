"""Generate the dbt models and schema.yml for us_dot_bts_ontime.

Everything is derived from the architecture CSVs, so the SQL, the YAML and the
staging parquet cannot drift from each other.

    uv run --no-project python models/us_dot_bts_ontime/code/gen_dbt.py
"""

from __future__ import annotations

import csv
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

DATASET = "us_dot_bts_ontime"
MODELS = ROOT / "models" / DATASET
ARCH = MODELS / "code" / "architecture"

FIRST_YEAR, LAST_YEAR = 1987, 2026

CAST = {
    "STRING": "safe_cast({c} as string)",
    "INT64": "safe_cast({c} as int64)",
    "FLOAT64": "safe_cast({c} as float64)",
    "DATE": "safe_cast({c} as date)",
    "TIME": "safe_cast({c} as time)",
    "DATETIME": "safe_cast({c} as datetime)",
}

TABLE_DESCRIPTION = {
    "flight": (
        "Desempenho de pontualidade de cada voo doméstico regular dos Estados "
        "Unidos reportado ao Bureau of Transportation Statistics, uma linha por "
        "voo programado desde outubro de 1987. Traz empresa aérea, aeroportos de "
        "origem e destino, horários programados e efetivos, atrasos na partida e "
        "na chegada, cancelamentos, desvios e a atribuição do atraso por causa. "
        "As causas do atraso existem a partir de junho de 2003 e as colunas de "
        "desvio a partir de 2008; antes disso são nulas por ausência na fonte, "
        "não por ausência de atraso. A combinação de data, empresa aérea, número "
        "do voo, origem, destino e horário programado de partida identifica uma "
        "linha em praticamente toda a série, mas não é uma chave primária global: "
        "voos desviados são registrados como dois trechos e produzem duplicatas "
        "residuais de até 0,01% em 1987 e 0,0006% em 2003."
    ),
    "airport": (
        "Aeroportos identificados pelo US DOT na base de pontualidade, com "
        "cidade, estado ou país e nome do aeroporto extraídos da tabela de "
        "consulta oficial do BTS. Serve de referência para as colunas "
        "origin_airport_id e destination_airport_id da tabela flight."
    ),
    "dicionario": (
        "Dicionário dos valores codificados das colunas categóricas do conjunto, "
        "montado a partir das tabelas de consulta publicadas pelo BTS."
    ),
}

# The flight table is 114 columns wide and runs to hundreds of millions of rows.
# not_null_proportion_multiple_columns compiles a scan of *every* column, so it
# is scoped to the most recent year; unscoped it would burn the BigQuery daily
# byte quota on its own.
TESTS = {
    "flight": textwrap.dedent("""\
        tests:
          - dbt_utils.unique_combination_of_columns:
              combination_of_columns:
                [flight_date, reporting_carrier, flight_number, origin, destination,
                 scheduled_departure_time]
              config:
                where: __most_recent_year_en__
          - not_null_proportion_multiple_columns:
              at_least: 0.05
              ignore_values:
                - cancellation_code
                - first_departure_time
                - total_additional_gate_time
                - longest_additional_gate_time
                - diverted_reached_destination
                - diverted_actual_elapsed_time
                - diverted_arrival_delay
                - diverted_distance
        {div_ignores}
              config:
                where: __most_recent_year_en__
          - custom_dictionary_coverage:
              columns_covered_by_dictionary:
                - cancellation_code
                - distance_group
                - day_of_week
              dictionary_model: ref('us_dot_bts_ontime__dicionario')
              config:
                where: __most_recent_year_en__
    """),
    "airport": textwrap.dedent("""\
        tests:
          - dbt_utils.unique_combination_of_columns:
              combination_of_columns: [airport_id]
          - not_null_proportion_multiple_columns:
              at_least: 0.05
              ignore_values: [state_abbreviation, country_name]
    """),
    "dicionario": textwrap.dedent("""\
        tests:
          - dbt_utils.unique_combination_of_columns:
              combination_of_columns: [id_tabela, nome_coluna, chave]
          - not_null_proportion_multiple_columns:
              at_least: 0.05
              ignore_values: [cobertura_temporal]
    """),
}

NOT_NULL = {
    "flight": {
        "year",
        "month",
        "flight_date",
        "reporting_carrier",
        "origin",
        "destination",
    },
    "airport": {"airport_id"},
    "dicionario": {"id_tabela", "nome_coluna", "chave", "valor"},
}


def read_arch(table: str) -> list[dict]:
    with open(ARCH / f"{table}.csv", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def sql_for(table: str) -> str:
    arch = read_arch(table)
    config = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table == "flight":
        config += [
            "        partition_by={",
            '            "field": "year",',
            '            "data_type": "int64",',
            f'            "range": {{"start": {FIRST_YEAR}, "end": {LAST_YEAR + 5}, "interval": 1}},',
            "        },",
            '        cluster_by=["reporting_carrier", "origin", "destination"],',
        ]
    body = ",\n".join(
        "    " + CAST[a["bigquery_type"]].format(c=a["name"]) + " " + a["name"]
        for a in arch
    )
    return (
        "{{\n    config(\n" + "\n".join(config) + "\n    )\n}}\n\n\n"
        "select\n" + body + "\nfrom\n"
        f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        "    as t\n"
    )


def yaml_for(table: str) -> str:
    arch = read_arch(table)
    div_ignores = "\n".join(
        f"        - diversion_{n}_{f}"
        for n in (1, 2, 3, 4, 5)
        for f in (
            "airport",
            "airport_id",
            "airport_seq_id",
            "wheels_on_time",
            "total_gate_time",
            "longest_gate_time",
            "wheels_off_time",
            "tail_number",
        )
    )
    tests = (
        TESTS[table].format(div_ignores=div_ignores)
        if table == "flight"
        else TESTS[table]
    )
    tests = textwrap.indent(tests.rstrip(), "    ")

    lines = [f"  - name: {DATASET}__{table}", "    description: >"]
    lines += [
        "      " + ln for ln in textwrap.wrap(TABLE_DESCRIPTION[table], 74)
    ]
    lines.append(tests)
    lines.append("    columns:")
    for a in arch:
        lines.append(f"      - name: {a['name']}")
        lines.append("        description: >")
        lines += [
            "          " + ln for ln in textwrap.wrap(a["description"], 70)
        ]
        if a["name"] in NOT_NULL[table]:
            lines.append("        tests: [not_null]")
    return "\n".join(lines)


def main() -> None:
    MODELS.mkdir(parents=True, exist_ok=True)
    tables = ["flight", "airport", "dicionario"]
    for t in tables:
        p = MODELS / f"{DATASET}__{t}.sql"
        p.write_text(sql_for(t), encoding="utf-8")
        print(f"{p.name}: {len(read_arch(t))} columns")

    schema = (
        "---\nversion: 2\nmodels:\n"
        + "\n".join(yaml_for(t) for t in tables)
        + "\n"
    )
    (MODELS / "schema.yml").write_text(schema, encoding="utf-8")
    print("schema.yml written")


if __name__ == "__main__":
    main()
