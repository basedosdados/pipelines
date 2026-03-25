import pandas as pd

from .shared import (
    ORDEM_BRASIL,
    apply_conta_split,
    get_unmatched,
    partition_and_save_brasil,
)

LEVEL = "brasil"
ANEXO = "DCA-Anexo I-D"


def build(path_dados, path_queries, comp, year_data, ano):
    df_comp = comp["despesas"]
    df = year_data.get(LEVEL, {}).get(ANEXO)
    if df is None or df.empty:
        print(f"  brasil_despesas_orcamentarias {ano}: no data, skipping")
        return pd.DataFrame()

    df = apply_conta_split(df)
    df["ano"] = str(ano)

    chaves = ["ano", "estagio", "portaria", "conta"]
    df = df[["ano", "estagio", "portaria", "conta", "valor"]].merge(
        df_comp, how="left", on=chaves
    )
    unmatched = get_unmatched(df)
    df["conta"] = df["conta"].astype("string")
    df = df.fillna("")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").astype("float")
    df = df[ORDEM_BRASIL]

    print(f"  brasil_despesas_orcamentarias {ano}: {len(df):,} rows")
    partition_and_save_brasil(
        df, "brasil_despesas_orcamentarias", ano, path_dados
    )
    return unmatched
