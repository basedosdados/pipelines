"""Remove entradas obsoletas do dicionário br_ms_sim (staging).

As colunas de microdados são recodificadas para texto no cleaning.py.
O dicionário legado mapeia chave numérica → rótulo, o que não reflete mais
a tabela e quebra a feature de tradução SQL na API.

Uso:
    export BD_SERVICE_ACCOUNT_DEV="$HOME/.basedosdados/credentials/staging.json"
    cd models/br_ms_sim/code

    # Conferir o que será removido (não altera nada)
    uv run python update_dicionario.py --dry-run

    # Aplicar: atualiza staging + roda dbt depois
    uv run python update_dicionario.py --apply
"""

from __future__ import annotations

import argparse
from pathlib import Path

import basedosdados as bd
import pandas as pd

DATASET_ID = "br_ms_sim"
TABLE_ID = "dicionario"
MICRODADOS_TABLE = "microdados"
BILLING_PROJECT = "basedosdados-dev"
STAGING_TABLE = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{TABLE_ID}"

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_CSV = OUTPUT_DIR / "dicionario.csv"

# Colunas categóricas de microdados recodificadas para texto (cleaning.py).
# naturalidade: tabela guarda nome do lugar (valor), não chave numérica.
COLUMNS_TO_REMOVE = [
    "acidente_trabalho",
    "assistencia_medica",
    "atestante",
    "circunstancia_obito",
    "cirurgia",
    "codificado",
    "escolaridade",
    "escolaridade_2010",
    "escolaridade_falecido_2010_agr",
    "escolaridade_mae",
    "escolaridade_mae_2010",
    "escolaridade_mae_2010_agr",
    "estado_civil",
    "exame",
    "fonte",
    "fonte_investigacao",
    "gestacao",
    "gravidez",
    "local_ocorrencia",
    "morte_parto",
    "naturalidade",
    "necropsia",
    "obito_gravidez",
    "obito_parto",
    "obito_puerperio",
    "parto",
    "raca_cor",
    "sexo",
    "status_codificadora",
    "status_do_epidem",
    "status_do_nova",
    "tipo_morte_ocorrencia",
    "tipo_nivel_investigador",
    "tipo_obito",
    "tipo_obito_ocorrencia",
    "tipo_pos",
    "tipo_resgate_informacao",
]


def load_staging_dictionary() -> pd.DataFrame:
    """Carrega o dicionário completo do staging."""
    sql = f"SELECT * FROM `{STAGING_TABLE}`"
    return bd.read_sql(
        sql,
        billing_project_id=BILLING_PROJECT,
        from_file=True,
    )


def column_name_series(df: pd.DataFrame) -> pd.Series:
    """Nome da coluna no staging (coluna ou nome_coluna)."""
    if "coluna" in df.columns:
        return df["coluna"].astype(str)
    if "nome_coluna" in df.columns:
        return df["nome_coluna"].astype(str)
    msg = "Staging dicionario sem coluna 'coluna' ou 'nome_coluna'"
    raise ValueError(msg)


def filter_dictionary(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separa linhas removidas e dicionário atualizado."""
    col_series = column_name_series(df)
    mask_remove = (
        df["id_tabela"].astype(str) == MICRODADOS_TABLE
    ) & col_series.isin(COLUMNS_TO_REMOVE)
    removed = df.loc[mask_remove].copy()
    kept = df.loc[~mask_remove].copy()
    return removed, kept


def summarize(removed: pd.DataFrame, kept: pd.DataFrame) -> None:
    """Imprime resumo da operação."""
    if removed.empty:
        print("Nenhuma linha a remover (dicionário já atualizado?).")
        return

    col_col = "coluna" if "coluna" in removed.columns else "nome_coluna"
    by_col = (
        removed.groupby(col_col, dropna=False)
        .size()
        .reset_index(name="n_linhas")
        .sort_values(col_col)
    )
    print(f"Linhas a remover: {len(removed)}")
    print(f"Linhas mantidas:  {len(kept)}")
    print("\nPor coluna (microdados):")
    print(by_col.to_string(index=False))


def upload_dictionary(path: Path) -> None:
    """Envia CSV atualizado para o staging."""
    table = bd.Table(dataset_id=DATASET_ID, table_id=TABLE_ID)
    table.create(
        path=str(path),
        if_table_exists="replace",
        if_storage_data_exists="replace",
    )
    print(f"Upload concluído: staging/{DATASET_ID}/{TABLE_ID}/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove entradas obsoletas do dicionário br_ms_sim."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Grava CSV e faz upload para o staging (padrão: só dry-run).",
    )
    args = parser.parse_args()

    print(f"Lendo {STAGING_TABLE} ...")
    df = load_staging_dictionary()
    removed, kept = filter_dictionary(df)
    summarize(removed, kept)

    if not args.apply:
        print(
            "\nDry-run. Para aplicar: uv run python update_dicionario.py --apply"
        )
        return

    if removed.empty:
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    kept.to_csv(OUTPUT_CSV, index=False)
    print(f"\nCSV salvo em {OUTPUT_CSV}")
    upload_dictionary(OUTPUT_CSV)
    print(
        "\nPróximo passo: cd ~/pipelines && "
        "uv run dbt run --select br_ms_sim__dicionario"
    )


if __name__ == "__main__":
    main()
