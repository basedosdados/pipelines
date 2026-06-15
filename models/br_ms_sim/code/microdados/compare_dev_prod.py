"""Compara nulls por ano (1996-2025) entre dev e prod no BigQuery.

Uso:
    cd ~/pipelines/models/br_ms_sim/code/microdados
    uv run python compare_dev_prod.py
"""

import basedosdados as bd
import pandas as pd

BILLING = "basedosdados-dev"
TABLES = {
    "dev": "`basedosdados-dev.br_ms_sim.microdados`",
    "prod": "`basedosdados.br_ms_sim.microdados`",
}

QUERY = """
SELECT
    ano,
    COUNT(*) AS n,
    COUNTIF(data_obito IS NULL) AS data_obito_null,
    COUNTIF(sexo IS NULL) AS sexo_null,
    COUNTIF(causa_basica IS NULL) AS causa_basica_null,
    COUNTIF(sequencial_obito IS NULL) AS sequencial_obito_null,
    MAX(data_obito) AS data_obito_max
FROM {table}
WHERE ano BETWEEN 1996 AND 2025
GROUP BY ano
ORDER BY ano
"""


def fetch(env: str) -> pd.DataFrame:
    df = bd.read_sql(
        QUERY.format(table=TABLES[env]),
        billing_project_id=BILLING,
        from_file=True,
    )
    return df.set_index("ano").add_prefix(f"{env}_")


def main() -> None:
    dev = fetch("dev")
    prod = fetch("prod")

    merged = dev.join(prod, how="outer").sort_index()

    print("\n" + "=" * 70)
    print("Cobertura + nulls por ano — dev x prod (1996-2025)")
    print("=" * 70)
    print(merged.to_string())

    null_cols = [
        "data_obito_null",
        "sexo_null",
        "causa_basica_null",
        "sequencial_obito_null",
    ]
    diff_mask = pd.Series(False, index=merged.index)
    for col in null_cols:
        diff_mask |= merged[f"dev_{col}"].fillna(-1) != merged[
            f"prod_{col}"
        ].fillna(-1)

    print("\n" + "=" * 70)
    print("Anos com diferenca dev x prod (nulls ou ausencia de ano)")
    print("=" * 70)
    diff = merged[diff_mask]
    print(
        diff.to_string() if not diff.empty else "Nenhuma diferenca encontrada."
    )

    print("\n" + "=" * 70)
    print("Totais de null por coluna")
    print("=" * 70)
    for col in null_cols:
        print(
            f"{col:24s} dev={int(merged[f'dev_{col}'].fillna(0).sum()):>10,}"
            f"  prod={int(merged[f'prod_{col}'].fillna(0).sum()):>10,}"
        )


if __name__ == "__main__":
    main()
