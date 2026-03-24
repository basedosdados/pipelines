import glob
import os

import pandas as pd

from .shared import (
    ORDEM_MUNICIPIO,
    apply_conta_split,
    load_api_json,
    partition_and_save,
)

ANEXO = "DCA-Anexo I-D"


def build(path_dados, path_queries, comp, api_dir, first_year, last_year):
    df_comp = comp["despesas"]

    for ano in range(first_year, last_year + 1):
        json_files = sorted(
            glob.glob(
                os.path.join(
                    api_dir,
                    f"dca_{ano}_[0-9][0-9][0-9][0-9][0-9][0-9][0-9].json",
                )
            )
        )
        if not json_files:
            print(
                f"  municipio_despesas_orcamentarias {ano}: no API files, skipping"
            )
            continue

        rows = []
        for jpath in json_files:
            df = load_api_json(jpath)
            if df.empty:
                continue
            df = df[df["anexo"] == ANEXO].copy()
            if df.empty:
                continue
            df = apply_conta_split(df, ano)
            df["ano"] = str(ano)
            rows.append(
                df[
                    [
                        "id_municipio",
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
                f"  municipio_despesas_orcamentarias {ano}: no data in API files, skipping"
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
        df_all = df_all[ORDEM_MUNICIPIO]

        print(
            f"  municipio_despesas_orcamentarias {ano}: {len(df_all):,} rows from {len(json_files)} municipalities"
        )
        partition_and_save(
            df_all, "municipio_despesas_orcamentarias", ano, path_dados
        )
