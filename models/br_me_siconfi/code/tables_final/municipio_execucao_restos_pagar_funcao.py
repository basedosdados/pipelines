import pandas as pd

from .shared import (
    ORDEM_MUNICIPIO_SIMPLES,
    apply_conta_split,
    partition_and_save,
)

LEVEL = "municipio"
ANEXO = "DCA-Anexo I-G"


def build(path_dados, path_queries, comp, year_data, ano):
    df = year_data.get(LEVEL, {}).get(ANEXO)
    if df is None or df.empty:
        print(
            f"  municipio_execucao_restos_pagar_funcao {ano}: no data, skipping"
        )
        return pd.DataFrame()

    df = apply_conta_split(df)
    df["portaria"] = df["portaria"].str.lstrip("0")
    df["ano"] = str(ano)
    df["estagio_bd"] = ""
    df["id_conta_bd"] = ""
    df["conta_bd"] = ""

    df = df[ORDEM_MUNICIPIO_SIMPLES]
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").astype("float")

    n = df["id_municipio"].nunique()
    print(
        f"  municipio_execucao_restos_pagar_funcao {ano}: {len(df):,} rows from {n} municipalities"
    )
    partition_and_save(
        df, "municipio_execucao_restos_pagar_funcao", ano, path_dados
    )
    return pd.DataFrame()
