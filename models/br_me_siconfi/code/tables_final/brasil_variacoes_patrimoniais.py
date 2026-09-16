import pandas as pd

from .shared import (
    ORDEM_BRASIL_VARIACOES,
    apply_conta_split,
    partition_and_save_brasil,
)

LEVEL = "brasil"
ANEXO = "DCA-Anexo I-HI"


def build(path_dados, path_queries, comp, year_data, ano):
    df = year_data.get(LEVEL, {}).get(ANEXO)
    if df is None or df.empty:
        print(f"  brasil_variacoes_patrimoniais {ano}: no data, skipping")
        return pd.DataFrame()

    df = df[df["estagio"] == f"31/12/{ano}"].copy()
    if df.empty:
        print(
            f"  brasil_variacoes_patrimoniais {ano}: no end-of-year data, skipping"
        )
        return pd.DataFrame()

    df = apply_conta_split(df)
    df["ano"] = str(ano)
    df["id_conta_bd"] = ""
    df["conta_bd"] = ""

    df = df[ORDEM_BRASIL_VARIACOES]
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").astype("float")

    print(f"  brasil_variacoes_patrimoniais {ano}: {len(df):,} rows")
    partition_and_save_brasil(
        df, "brasil_variacoes_patrimoniais", ano, path_dados
    )
    return pd.DataFrame()
