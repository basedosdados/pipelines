import glob
import os

import pandas as pd

from .shared import (
    ORDEM_MUNICIPIO_BALANCO,
    apply_conta_split,
    load_api_json,
    partition_and_save,
)

ANEXO = "DCA-Anexo I-AB"


def build(path_dados, path_queries, comp, api_dir, first_year, last_year):
    df_comp = comp["balanco"]

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
                f"  municipio_balanco_patrimonial {ano}: no API files, skipping"
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
            # Validate that coluna is end-of-year (balanco is a stock measure)
            df = df[df["coluna"] == f"31/12/{ano}"].copy()
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
                        "portaria",
                        "conta",
                        "valor",
                    ]
                ]
            )

        if not rows:
            print(
                f"  municipio_balanco_patrimonial {ano}: no data in API files, skipping"
            )
            continue

        df_all = pd.concat(rows, ignore_index=True)
        df_all["ano"] = str(ano)

        # Merge key for balanco 2013+: ["ano", "portaria"]
        chaves = ["ano", "portaria"]
        df_all = df_all.merge(df_comp, how="left", on=chaves)

        # Resolve conta conflict from merge (df has conta, comp also has conta)
        if "conta_x" in df_all.columns:
            df_all["conta"] = df_all["conta_x"]
            df_all = df_all.drop(["conta_x", "conta_y"], axis=1)

        df_all["conta"] = df_all["conta"].astype("string")
        df_all = df_all.fillna("")
        df_all["valor"] = pd.to_numeric(
            df_all["valor"], errors="coerce"
        ).astype("float")
        df_all = df_all[ORDEM_MUNICIPIO_BALANCO]

        print(
            f"  municipio_balanco_patrimonial {ano}: {len(df_all):,} rows from {len(json_files)} municipalities"
        )
        partition_and_save(
            df_all, "municipio_balanco_patrimonial", ano, path_dados
        )
