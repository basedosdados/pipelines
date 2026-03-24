import glob
import os

import pandas as pd

from .shared import (
    ORDEM_UF,
    apply_conta_split_funcao,
    load_api_json_uf,
    partition_and_save,
)

ANEXO = "DCA-Anexo I-E"


def build(path_dados, path_queries, comp, api_dir, first_year, last_year):
    df_comp = comp["despesas_funcao"]

    for ano in range(first_year, last_year + 1):
        json_files = sorted(
            glob.glob(os.path.join(api_dir, f"dca_{ano}_[0-9][0-9].json"))
        )
        if not json_files:
            print(f"  uf_despesas_funcao {ano}: no API files, skipping")
            continue

        rows = []
        for jpath in json_files:
            df = load_api_json_uf(jpath)
            if df.empty:
                continue
            df = df[df["anexo"] == ANEXO].copy()
            if df.empty:
                continue
            df = apply_conta_split_funcao(df)
            df["portaria"] = df["portaria"].str.lstrip("0")
            df["ano"] = str(ano)
            rows.append(
                df[
                    [
                        "id_uf",
                        "sigla_uf",
                        "ano",
                        "estagio",
                        "portaria",
                        "conta",
                        "valor",
                    ]
                ]
            )

        if not rows:
            print(
                f"  uf_despesas_funcao {ano}: no data in API files, skipping"
            )
            continue

        df_all = pd.concat(rows, ignore_index=True)
        df_all["ano"] = str(ano)

        chaves = ["ano", "estagio", "portaria", "conta"]
        df_all = df_all.merge(df_comp, how="left", on=chaves)
        df_all["conta"] = df_all["conta"].astype("string")
        df_all = df_all.fillna("")
        df_all["valor"] = pd.to_numeric(
            df_all["valor"], errors="coerce"
        ).astype("float")
        df_all = df_all[ORDEM_UF]

        n_states = df_all["id_uf"].nunique()
        print(
            f"  uf_despesas_funcao {ano}: {len(df_all):,} rows from {n_states} states"
        )
        partition_and_save(df_all, "uf_despesas_funcao", ano, path_dados)
