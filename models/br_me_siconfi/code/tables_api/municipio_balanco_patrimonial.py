import pandas as pd

from .shared import (
    ORDEM_MUNICIPIO_BALANCO,
    apply_conta_split,
    get_unmatched,
    partition_and_save,
)

LEVEL = "municipio"
ANEXO = "DCA-Anexo I-AB"


def build(path_dados, path_queries, comp, year_data, ano):
    df_comp = comp["balanco"]
    df = year_data.get(LEVEL, {}).get(ANEXO)
    if df is None or df.empty:
        print(f"  municipio_balanco_patrimonial {ano}: no data, skipping")
        return pd.DataFrame()

    # Balanco is a stock measure — keep only end-of-year snapshot
    df = df[df["estagio"] == f"31/12/{ano}"].copy()
    if df.empty:
        print(
            f"  municipio_balanco_patrimonial {ano}: no end-of-year data, skipping"
        )
        return pd.DataFrame()

    df = apply_conta_split(df)
    df["ano"] = str(ano)

    chaves = ["ano", "portaria"]
    df = df[
        ["id_municipio", "sigla_uf", "ano", "portaria", "conta", "valor"]
    ].merge(df_comp, how="left", on=chaves)
    # Resolve conta conflict: keep the API's conta value
    if "conta_x" in df.columns:
        df["conta"] = df["conta_x"]
        df = df.drop(["conta_x", "conta_y"], axis=1)

    unmatched = get_unmatched(df, keys=("ano", "portaria", "conta"))
    df["conta"] = df["conta"].astype("string")
    df = df.fillna("")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").astype("float")
    df = df[ORDEM_MUNICIPIO_BALANCO]

    n = df["id_municipio"].nunique()
    print(
        f"  municipio_balanco_patrimonial {ano}: {len(df):,} rows from {n} municipalities"
    )
    partition_and_save(df, "municipio_balanco_patrimonial", ano, path_dados)
    return unmatched
