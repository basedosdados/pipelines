"""Cruza investigação do .dbc com a tabela materializada no BigQuery."""

import basedosdados as bd

TABLE = "`basedosdados-dev.br_ms_sim.microdados`"
UFS = ("SP", "AC")
YEARS = (2020, 2021, 2022, 2023, 2024)


def run(label: str, query: str) -> None:
    print(f"\n{'=' * 60}\n{label}\n{'=' * 60}")
    df = bd.read_sql(
        query, billing_project_id="basedosdados-dev", from_file=True
    )
    print(df.to_string(index=False))


def main() -> None:
    ufs_sql = ", ".join(f"'{u}'" for u in UFS)
    years_sql = ", ".join(str(y) for y in YEARS)

    run(
        "Nulls e distintos por ano/UF (comparável ao .dbc)",
        f"""
        SELECT
            ano,
            sigla_uf,
            COUNT(*) AS n,
            COUNTIF(sequencial_obito IS NULL) AS sequencial_obito_null,
            COUNT(DISTINCT sequencial_obito) AS sequencial_obito_distintos,
            COUNTIF(data_obito IS NULL) AS data_obito_null,
            COUNTIF(causa_basica IS NULL) AS causa_basica_null,
            COUNTIF(sexo IS NULL) AS sexo_null
        FROM {TABLE}
        WHERE sigla_uf IN ({ufs_sql})
          AND ano IN ({years_sql})
        GROUP BY ano, sigla_uf
        ORDER BY sigla_uf, ano
        """,
    )

    run(
        "Resumo por ano (todas as UFs)",
        f"""
        SELECT
            ano,
            COUNT(*) AS n,
            COUNTIF(sequencial_obito IS NULL) AS sequencial_obito_null,
            COUNTIF(data_obito IS NULL) AS data_obito_null,
            COUNTIF(causa_basica IS NULL) AS causa_basica_null,
            COUNTIF(sexo IS NULL) AS sexo_null
        FROM {TABLE}
        WHERE ano IN ({years_sql})
        GROUP BY ano
        ORDER BY ano
        """,
    )

    run(
        "Registro(s) com causa_basica null",
        f"""
        SELECT ano, sigla_uf, sequencial_obito, causa_basica, data_obito, sexo
        FROM {TABLE}
        WHERE causa_basica IS NULL
        LIMIT 10
        """,
    )

    run(
        "Unicidade: combinações duplicadas (ano, sigla_uf, sequencial_obito)",
        f"""
        SELECT ano, sigla_uf, sequencial_obito, COUNT(*) AS vezes
        FROM {TABLE}
        GROUP BY ano, sigla_uf, sequencial_obito
        HAVING COUNT(*) > 1
        ORDER BY ano, sigla_uf
        LIMIT 30
        """,
    )


if __name__ == "__main__":
    main()
