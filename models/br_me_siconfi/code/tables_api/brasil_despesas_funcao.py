import os

import pandas as pd

from .shared import (
    ORDEM_BRASIL,
    apply_conta_split_funcao,
    load_api_json_brasil,
    partition_and_save_brasil,
)

ANEXO = "DCA-Anexo I-E"


def build(path_dados, path_queries, comp, api_dir, first_year, last_year):
    df_comp = comp["despesas_funcao"]

    for ano in range(first_year, last_year + 1):
        json_path = os.path.join(api_dir, f"dca_{ano}_1.json")
        if not os.path.exists(json_path):
            print(f"  brasil_despesas_funcao {ano}: no API file, skipping")
            continue

        df = load_api_json_brasil(json_path)
        if df.empty:
            print(f"  brasil_despesas_funcao {ano}: no data, skipping")
            continue

        df = df[df["anexo"] == ANEXO].copy()
        if df.empty:
            print(
                f"  brasil_despesas_funcao {ano}: no data for anexo, skipping"
            )
            continue

        df = apply_conta_split_funcao(df)
        df["portaria"] = df["portaria"].str.lstrip("0")
        df["ano"] = str(ano)

        chaves = ["ano", "estagio", "portaria", "conta"]
        df = df.merge(df_comp, how="left", on=chaves)
        df["conta"] = df["conta"].astype("string")
        df = df.fillna("")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce").astype(
            "float"
        )
        df = df[ORDEM_BRASIL]

        print(f"  brasil_despesas_funcao {ano}: {len(df):,} rows")
        partition_and_save_brasil(
            df, "brasil_despesas_funcao", ano, path_dados
        )
