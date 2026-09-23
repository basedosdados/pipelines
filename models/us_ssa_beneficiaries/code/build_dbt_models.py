"""Generate the dbt SQL models and schema.yml for us_ssa_beneficiaries.

Both are derived from the architecture CSVs, which are the source of truth for
column names, order and types.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml  # pyrefly: ignore[untyped-import]

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
MODELS = HERE.parent
DATASET = "us_ssa_beneficiaries"

PARTITION_RANGE = {"start": 1998, "end": 2030, "interval": 1}

TABLE_DESCRIPTIONS = {
    "oasdi_county": (
        "Número de beneficiários do OASDI (Old-Age, Survivors, and Disability "
        "Insurance) em current-payment status e valor total dos benefícios pagos, "
        "por condado, ano, tipo de benefício, faixa etária e sexo. Formato longo: "
        "cada linha é uma combinação de condado, ano e categoria. As linhas de "
        "total estadual publicadas pela fonte foram movidas para oasdi_state."
    ),
    "oasdi_state": (
        "Número de beneficiários do OASDI em current-payment status e valor total "
        "dos benefícios pagos, por estado ou área, ano, tipo de benefício, faixa "
        "etária e sexo. Inclui territórios, o total nacional ('All areas') e as "
        "categorias residuais 'Other', 'Foreign countries' e 'Unknown'."
    ),
    "oasdi_population_share": (
        "População residente estimada e percentual dela que recebe benefícios do "
        "OASDI, por estado ou área e ano, para a população total e para a "
        "população de 65 anos ou mais."
    ),
    "ssi_county": (
        "Número de recebedores do SSI (Supplemental Security Income) e valor total "
        "dos pagamentos, por condado, ano, categoria de elegibilidade, faixa etária "
        "e recebimento simultâneo de OASDI. Formato longo. O valor dos pagamentos "
        "só é publicado para o total do condado."
    ),
    "ssi_state": (
        "Número de recebedores do SSI e valor total dos pagamentos, por estado ou "
        "área, ano, categoria de elegibilidade, faixa etária e recebimento "
        "simultâneo de OASDI. Inclui territórios e o total nacional ('All areas')."
    ),
    "dicionario": (
        "Dicionário de valores para as colunas codificadas das tabelas do conjunto."
    ),
}

# The logical key of each table, for the uniqueness test.
#
# The county tables key on state_id + county_name + county_id, not county_id
# alone. county_id is NULL for the handful of rows SSA publishes with no
# resolvable code -- the "Unknown" county rows and entities abolished before
# ANSI codes were first published -- and dbt's uniqueness test groups NULLs
# together, so county_id alone collides across those rows. Adding the name and
# the state separates them, while county_id still separates Baltimore county
# from Baltimore city, which share a name and a state.
UNIQUE_KEYS = {
    "oasdi_county": [
        "year",
        "state_id",
        "county_name",
        "county_id",
        "benefit_type",
        "age_group",
        "sex",
    ],
    "oasdi_state": [
        "year",
        "state_or_area",
        "benefit_type",
        "age_group",
        "sex",
    ],
    "oasdi_population_share": ["year", "state_or_area", "population_group"],
    "ssi_county": [
        "year",
        "state_id",
        "county_name",
        "county_id",
        "eligibility_category",
        "age_group",
        "oasdi_concurrent",
    ],
    "ssi_state": [
        "year",
        "state_or_area",
        "eligibility_category",
        "age_group",
        "oasdi_concurrent",
    ],
}

NOT_NULL = {
    "oasdi_county": [
        "year",
        "state_id",
        "state_name",
        "county_name",
        "benefit_type",
        "age_group",
        "sex",
    ],
    "oasdi_state": [
        "year",
        "state_or_area",
        "benefit_type",
        "age_group",
        "sex",
    ],
    "oasdi_population_share": ["year", "state_or_area", "population_group"],
    "ssi_county": [
        "year",
        "state_id",
        "state_name",
        "county_name",
        "eligibility_category",
        "age_group",
        "oasdi_concurrent",
    ],
    "ssi_state": [
        "year",
        "state_or_area",
        "eligibility_category",
        "age_group",
        "oasdi_concurrent",
    ],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

# Columns that are legitimately sparse, so the not-null-proportion test would
# otherwise fail on them. Measured, not guessed: everything else in these
# tables is above 93% non-null, including county_id (99.99% / 99.49%) and
# state_id, so they stay under test -- they are the most defect-prone columns
# in the dataset and excluding them would gut the check.
#
# The `*_note` columns are near-empty by design: a note is only set when a
# value is missing. `ssi_county.payment_amount_month` is 14.15% non-null
# because SSA publishes the county payment total only for the total category,
# one of seven.
IGNORE_SPARSE = {
    "oasdi_county": ["beneficiary_count_note", "benefit_amount_month_note"],
    "oasdi_state": ["beneficiary_count_note", "benefit_amount_month_note"],
    "oasdi_population_share": [
        "population_note",
        "percentage_receiving_oasdi_note",
    ],
    "ssi_county": [
        "recipient_count_note",
        "payment_amount_month",
        "payment_amount_month_note",
    ],
    "ssi_state": ["recipient_count_note", "payment_amount_month_note"],
    "dicionario": ["cobertura_temporal"],
}

CAST = {"INT64": "int64", "FLOAT64": "float64", "STRING": "string"}


def sql_for(table: str, columns: list[tuple[str, str]]) -> str:
    cfg = [
        f'        alias="{table}",',
        f'        schema="{DATASET}",',
        '        materialized="table",',
    ]
    if any(n == "year" for n, _ in columns):
        cfg.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {PARTITION_RANGE["start"]}, '
            f'"end": {PARTITION_RANGE["end"]}, "interval": {PARTITION_RANGE["interval"]}}},\n'
            "        },"
        )
    body = ",\n".join(
        f"    safe_cast({n} as {CAST[t]}) {n}" for n, t in columns
    )
    return (
        "{{\n    config(\n" + "\n".join(cfg) + "\n    )\n}}\n\n\n"
        "select\n" + body + "\nfrom\n"
        f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        "    as t\n"
    )


def main() -> None:
    arch_files = sorted(ARCH.glob("*.csv"))
    models = []
    for path in arch_files:
        table = path.stem
        arch = pd.read_csv(path, dtype=str).fillna("")
        columns = list(zip(arch["name"], arch["bigquery_type"], strict=True))
        (MODELS / f"{DATASET}__{table}.sql").write_text(
            sql_for(table, columns)
        )

        col_entries = []
        for _, row in arch.iterrows():
            entry = {"name": row["name"], "description": row["description"]}
            tests: list[object] = []
            if row["name"] in NOT_NULL.get(table, []):
                tests.append("not_null")
            if row["directory_column"] and row["name"] == "state_id":
                tests.append(
                    {
                        "relationships": {
                            "to": "ref('br_bd_diretorios_us__state')",
                            "field": "id_state",
                        }
                    }
                )
            if tests:
                entry["tests"] = tests
            col_entries.append(entry)

        model_tests: list[dict[str, object]] = []
        if table in UNIQUE_KEYS:
            model_tests.append(
                {
                    "dbt_utils.unique_combination_of_columns": {
                        "combination_of_columns": UNIQUE_KEYS[table]
                    }
                }
            )
        else:
            model_tests.append(
                {
                    "dbt_utils.unique_combination_of_columns": {
                        "combination_of_columns": [
                            "id_tabela",
                            "nome_coluna",
                            "chave",
                        ]
                    }
                }
            )
        sparse = IGNORE_SPARSE.get(table)
        proportion = (
            {"at_least": 0.05, "ignore_values": sparse}
            if sparse
            else {"at_least": 0.05}
        )
        model_tests.append(
            {"not_null_proportion_multiple_columns": proportion}
        )

        # Assert every dictionary-covered value in the built table has a row in
        # the dicionario. The dictionary is derived from the data, so it cannot
        # drift in Python -- this checks the same thing in BigQuery, where a
        # stale dicionario upload would show up.
        covered = [
            r["name"]
            for _, r in arch.iterrows()
            if r["covered_by_dictionary"] == "yes"
        ]
        if covered:
            model_tests.append(
                {
                    "custom_dictionary_coverage": {
                        "dictionary_model": f"ref('{DATASET}__dicionario')",
                        "columns_covered_by_dictionary": covered,
                    }
                }
            )

        models.append(
            {
                "name": f"{DATASET}__{table}",
                "description": TABLE_DESCRIPTIONS[table],
                "tests": model_tests,
                "columns": col_entries,
            }
        )

    schema = {"version": 2, "models": models}
    text = yaml.dump(
        schema,
        allow_unicode=True,
        sort_keys=False,
        width=88,
        default_flow_style=False,
    )
    (MODELS / "schema.yml").write_text("---\n" + text)
    print(f"wrote {len(models)} models + schema.yml")


if __name__ == "__main__":
    main()
