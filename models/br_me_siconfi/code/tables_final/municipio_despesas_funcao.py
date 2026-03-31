"""municipio_despesas_funcao builder.

Sources:
  1996-2012: input/municipio/quadro{ano}_5.xlsx  (Excel wide format)
             IBGE construction varies by sub-period:
               1996-1997: municipio_original + sigla_uf lookup
               1998-2003: CD_UF + CD_MUN
               2004-2012: UF + Cod Mun  (different column names)
  2013+:     API JSON  dca_{ano}_{cod_ibge}.json
"""

import glob
import os

import pandas as pd

from .shared import (
    ORDEM_MUNICIPIO,
    _apply_currency_conversion,
    apply_conta_split,
    build_ibge_id_1998_2012,
    get_unmatched,
    partition_and_save,
)

LEVEL = "municipio"
ANEXO = "DCA-Anexo I-E"


def _build_ibge_id_2004_2012(df, df_comp_municipio):
    """Variant of build_ibge_id_1998_2012 using 'UF'/'Cod Mun' column names."""
    df = df.copy()
    df["length"] = df["Cod Mun"].str.len()
    df["id_municipio_6"] = "0"
    df.loc[df["length"] == 1, "id_municipio_6"] = (
        df["UF"] + "000" + df["Cod Mun"]
    )
    df.loc[df["length"] == 2, "id_municipio_6"] = (
        df["UF"] + "00" + df["Cod Mun"]
    )
    df.loc[df["length"] == 3, "id_municipio_6"] = (
        df["UF"] + "0" + df["Cod Mun"]
    )
    df.loc[df["length"] == 4, "id_municipio_6"] = df["UF"] + df["Cod Mun"]

    comp_reduced = df_comp_municipio[["id_municipio", "sigla_uf"]].copy()
    comp_reduced["id_municipio_6"] = comp_reduced["id_municipio"].str[:6]

    df = df.merge(comp_reduced, how="left", on="id_municipio_6")
    df = df.drop(["length", "id_municipio_6"], axis=1)
    return df


def build(path_dados, path_queries, comp, year_data, ano):
    if ano <= 2012:
        return _build_legacy(path_dados, comp, ano)
    return _build_api(path_dados, comp, year_data, ano)


# ---------------------------------------------------------------------------
# Legacy path (1996-2012)
# ---------------------------------------------------------------------------


def _build_legacy(path_dados, comp, ano):
    pd.options.mode.chained_assignment = None
    df_comp_municipio = comp["municipio"]
    df_comp_despesas_funcao = comp["despesas_funcao"]

    arquivos = [
        arq
        for arq in glob.iglob(
            os.path.join(path_dados, "input/municipio/quadro*")
        )
        if int(arq[-11:-7]) == ano and int(arq[-6:-5]) == 5
    ]

    if not arquivos:
        print(
            f"  municipio_despesas_funcao {ano}: no legacy files found, skipping"
        )
        return pd.DataFrame()

    parts = []
    for arquivo in arquivos:
        df_dados = pd.read_excel(arquivo, dtype="string", na_values="")

        if 1996 <= ano <= 1997:
            df_dados["municipio_original"] = df_dados[
                "municipio_original"
            ].str.strip()
            df_dados = df_dados.merge(
                df_comp_municipio,
                how="left",
                on=["municipio_original", "sigla_uf"],
            )
            id_vars = [
                "id_municipio",
                "sigla_uf",
                "municipio_auxiliar",
                "municipio_original",
            ]
            to_drop = ["municipio_auxiliar", "municipio_original"]
        elif 1998 <= ano <= 2003:
            df_dados = build_ibge_id_1998_2012(df_dados, df_comp_municipio)
            df_dados = df_dados.drop_duplicates(
                subset=["id_municipio"], keep="first"
            )
            id_vars = ["id_municipio", "sigla_uf", "CD_UF", "CD_MUN"]
            to_drop = ["CD_UF", "CD_MUN"]
        else:  # 2004-2012
            df_dados = _build_ibge_id_2004_2012(df_dados, df_comp_municipio)
            df_dados = df_dados.drop_duplicates(
                subset=["id_municipio"], keep="first"
            )
            id_vars = ["id_municipio", "sigla_uf", "UF", "Cod Mun"]
            to_drop = ["UF", "Cod Mun"]

        value_vars = df_dados.drop(id_vars, axis=1).columns
        df_dados = pd.melt(
            df_dados,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="conta",
            value_name="valor",
        )
        df_dados = df_dados.drop(to_drop, axis=1)
        df_dados["ano"] = str(ano)

        df_dados = df_dados.merge(
            df_comp_despesas_funcao, how="left", on=["ano", "conta"]
        )
        df_dados["conta"] = df_dados["conta"].astype("string")
        df_dados = df_dados.fillna("")
        df_dados["valor"] = pd.to_numeric(
            df_dados["valor"], errors="coerce"
        ).astype("float")
        df_dados = _apply_currency_conversion(df_dados, ano)
        parts.append(df_dados[ORDEM_MUNICIPIO])

    df = pd.concat(parts, ignore_index=True)
    print(f"  municipio_despesas_funcao {ano}: {len(df):,} rows (legacy)")
    partition_and_save(df, "municipio_despesas_funcao", ano, path_dados)
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# API path (2013+)
# ---------------------------------------------------------------------------


def _build_api(path_dados, comp, year_data, ano):
    df_comp = comp["despesas_funcao"]
    df = year_data.get(LEVEL, {}).get(ANEXO)
    if df is None or df.empty:
        print(f"  municipio_despesas_funcao {ano}: no data, skipping")
        return pd.DataFrame()

    df = apply_conta_split(df)
    df["portaria"] = df["portaria"].str.lstrip("0")
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
        f"  municipio_despesas_funcao {ano}: {len(df):,} rows from {n} municipalities"
    )
    partition_and_save(df, "municipio_despesas_funcao", ano, path_dados)
    return unmatched
