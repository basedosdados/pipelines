import pandas as pd

from .shared import (
    ORDEM_MUNICIPIO,
    apply_conta_split,
    get_unmatched,
    partition_and_save,
)

LEVEL = "municipio"
ANEXO = "DCA-Anexo I-C"


def build(path_dados, path_queries, comp, year_data, ano):
    df_comp = comp["receitas"]
    df = year_data.get(LEVEL, {}).get(ANEXO)
    if df is None or df.empty:
        print(f"  municipio_receitas_orcamentarias {ano}: no data, skipping")
        return pd.DataFrame()

    df = apply_conta_split(df)
    df["ano"] = str(ano)

    chaves = ["ano", "estagio", "portaria", "conta"]
    df = df[
        [
            "id_municipio",
            "sigla_uf",
            "ano",
            "estagio",
            "portaria",
            "conta",
            "valor",
        ]
    ].merge(df_comp, how="left", on=chaves)
    unmatched = get_unmatched(df)
    df["conta"] = df["conta"].astype("string")
    df = df.fillna("")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").astype("float")
    df = df[ORDEM_MUNICIPIO]

    n = df["id_municipio"].nunique()
    print(
        f"  municipio_receitas_orcamentarias {ano}: {len(df):,} rows from {n} municipalities"
    )
    partition_and_save(df, "municipio_receitas_orcamentarias", ano, path_dados)
    return unmatched
