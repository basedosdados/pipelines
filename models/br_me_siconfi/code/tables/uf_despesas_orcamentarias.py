import os

import pandas as pd

from .shared import (
    ORDEM_UF,
    apply_conta_split,
    clean_finbra_uf,
    partition_and_save,
    read_finbra_csv,
    unzip_finbra,
)


def build(path_dados, path_queries, comp, first_year, last_year):
    pd.options.mode.chained_assignment = None

    df_comp_despesas = comp["despesas"]

    for ano in range(max(first_year, 2013), last_year + 1):
        df = pd.DataFrame(columns=ORDEM_UF)

        anexo = "finbra_ESTDF_DespesasOrcamentarias(AnexoI-D)"
        zip_path = os.path.join(path_dados, f"input/{anexo}/{ano}.zip")
        folder = os.path.join(path_dados, f"input/{anexo}")
        csv_path = unzip_finbra(zip_path, folder)

        df_dados = read_finbra_csv(csv_path)
        os.remove(csv_path)

        df_dados = clean_finbra_uf(df_dados)
        df_dados = apply_conta_split(df_dados, ano)

        df_dados["ano"] = str(ano)
        chaves = ["ano", "estagio", "portaria", "conta"]

        df_dados = df_dados.merge(df_comp_despesas, how="left", on=chaves)
        df_dados["conta"] = df_dados["conta"].astype("string")
        df_dados = df_dados.fillna("")
        df_dados["valor"] = pd.to_numeric(
            df_dados["valor"], errors="coerce"
        ).astype("float")

        df_dados = df_dados[ORDEM_UF]
        df = pd.concat([df, df_dados], ignore_index=True)

        print(f"Particionando uf_despesas_orcamentarias {ano}")
        partition_and_save(df, "uf_despesas_orcamentarias", ano, path_dados)
