"""municipio_balanco_patrimonial builder.

Sources:
  1998-2012: input/municipio/quadro{ano}_6.xlsx (ativo)
             input/municipio/quadro{ano}_7.xlsx (passivo)
             Merge key: ["ano", "categoria", "conta"]  (categoria = "ativo"/"passivo")
  2013+:     API JSON  dca_{ano}_{cod_ibge}.json
             Keep only end-of-year snapshot (estagio == "31/12/{ano}")
             Merge key: ["ano", "portaria"]

Output schema (no estagio column):
  ano, sigla_uf, id_municipio, portaria, conta, id_conta_bd, conta_bd, valor
"""

import glob
import os

import pandas as pd

from .shared import (
    ORDEM_MUNICIPIO_BALANCO,
    apply_conta_split,
    build_ibge_id_1998_2012,
    get_unmatched,
    partition_and_save,
)

LEVEL = "municipio"
ANEXO = "DCA-Anexo I-AB"

_FILE_CATEGORIA = {6: "ativo", 7: "passivo"}


def build(path_dados, path_queries, comp, year_data, ano):
    if ano <= 2012:
        return _build_legacy(path_dados, comp, ano)
    return _build_api(path_dados, comp, year_data, ano)


# ---------------------------------------------------------------------------
# Legacy path (1998-2012)
# ---------------------------------------------------------------------------


def _build_legacy(path_dados, comp, ano):
    pd.options.mode.chained_assignment = None
    df_comp_municipio = comp["municipio"]
    df_comp_balanco = comp["balanco"]

    arquivos = [
        arq
        for arq in glob.iglob(
            os.path.join(path_dados, "input/municipio/quadro*")
        )
        if int(arq[-11:-7]) == ano and 6 <= int(arq[-6:-5]) <= 7
    ]

    if not arquivos:
        print(
            f"  municipio_balanco_patrimonial {ano}: no legacy files found, skipping"
        )
        return pd.DataFrame()

    parts = []
    for arquivo in sorted(arquivos):
        file_num = int(arquivo[-6:-5])
        categoria = _FILE_CATEGORIA.get(file_num, "")

        df_dados = pd.read_excel(arquivo, dtype="string", na_values="")

        # 2009-2012 files use CdUF/CdMun column names and have extra metadata cols.
        if "CdUF" in df_dados.columns:
            df_dados = df_dados.rename(
                columns={"CdUF": "CD_UF", "CdMun": "CD_MUN"}
            )
            df_dados = df_dados.drop(
                columns=["Populacao", "sigla_uf", "municipio_original"],
                errors="ignore",
            )

        df_dados = build_ibge_id_1998_2012(df_dados, df_comp_municipio)
        df_dados = df_dados.drop_duplicates(
            subset=["id_municipio"], keep="first"
        )

        id_vars = ["id_municipio", "sigla_uf", "CD_UF", "CD_MUN"]
        value_vars = df_dados.drop(id_vars, axis=1).columns
        df_dados = pd.melt(
            df_dados,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="conta",
            value_name="valor",
        )
        df_dados = df_dados.drop(["CD_UF", "CD_MUN"], axis=1)

        df_dados["ano"] = str(ano)
        df_dados["categoria"] = categoria

        df_dados = df_dados.merge(
            df_comp_balanco, how="left", on=["ano", "categoria", "conta"]
        )
        df_dados = df_dados.drop("categoria", axis=1)

        # portaria column for legacy balanco: use empty string (no code in source data)
        if "portaria" not in df_dados.columns:
            df_dados["portaria"] = ""

        df_dados["conta"] = df_dados["conta"].astype("string")
        df_dados = df_dados.fillna("")
        df_dados["valor"] = pd.to_numeric(
            df_dados["valor"], errors="coerce"
        ).astype("float")
        parts.append(df_dados[ORDEM_MUNICIPIO_BALANCO])

    df = pd.concat(parts, ignore_index=True)
    print(f"  municipio_balanco_patrimonial {ano}: {len(df):,} rows (legacy)")
    partition_and_save(df, "municipio_balanco_patrimonial", ano, path_dados)
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# API path (2013+)
# ---------------------------------------------------------------------------


def _build_api(path_dados, comp, year_data, ano):
    df_comp = comp["balanco"]
    df = year_data.get(LEVEL, {}).get(ANEXO)
    if df is None or df.empty:
        print(f"  municipio_balanco_patrimonial {ano}: no data, skipping")
        return pd.DataFrame()

    # Balanco is a stock measure — keep only end-of-year snapshot.
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
    # Resolve conta conflict: keep the API's conta value.
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
