"""municipio_despesas_orcamentarias builder.

Sources:
  1989-2012: input/municipio/quadro{ano}_{N}.xlsx  (Excel wide format)
             file numbers: 4 (1989-1996), 3 (1997), 2 (1998-2008), 2-4 (2009-2012)
             files 2/3/4 for 2009-2012 correspond to estagio Empenhado/Liquidado/Pago
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
ANEXO = "DCA-Anexo I-D"

_ESTAGIO_MAP = {
    2: "Despesas Empenhadas",
    3: "Despesas Liquidadas",
    4: "Despesas Pagas",
}


def build(path_dados, path_queries, comp, year_data, ano):
    if ano <= 2012:
        return _build_legacy(path_dados, comp, ano)
    return _build_api(path_dados, comp, year_data, ano)


# ---------------------------------------------------------------------------
# Legacy path (1989-2012)
# ---------------------------------------------------------------------------


def _build_legacy(path_dados, comp, ano):
    pd.options.mode.chained_assignment = None
    df_comp_municipio = comp["municipio"]
    df_comp_despesas = comp["despesas"]

    arquivos = [
        arq
        for arq in glob.iglob(
            os.path.join(path_dados, "input/municipio/quadro*")
        )
        if int(arq[-11:-7]) == ano
        and (
            (1989 <= ano <= 1996 and int(arq[-6:-5]) == 4)
            or (ano == 1997 and int(arq[-6:-5]) == 3)
            or (1998 <= ano <= 2008 and int(arq[-6:-5]) == 2)
            or (2009 <= ano <= 2012 and 2 <= int(arq[-6:-5]) <= 4)
        )
    ]

    if not arquivos:
        print(
            f"  municipio_despesas_orcamentarias {ano}: no legacy files found, skipping"
        )
        return pd.DataFrame()

    parts = []
    for arquivo in sorted(arquivos):
        tipo_arquivo = int(arquivo[-6:-5])
        df_dados = pd.read_excel(arquivo, dtype="string", na_values="")

        if 1989 <= ano <= 1997:
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
        else:
            df_dados = build_ibge_id_1998_2012(df_dados, df_comp_municipio)
            df_dados = df_dados.drop_duplicates(
                subset=["id_municipio"], keep="first"
            )
            id_vars = ["id_municipio", "sigla_uf", "CD_UF", "CD_MUN"]
            to_drop = ["CD_UF", "CD_MUN"]

        # For 2009-2012 the file number identifies the estagio; set it before melt.
        if 2009 <= ano <= 2012:
            df_dados["estagio"] = _ESTAGIO_MAP.get(tipo_arquivo, "")

        value_vars = df_dados.drop(
            id_vars + (["estagio"] if "estagio" in df_dados.columns else []),
            axis=1,
        ).columns
        df_dados = pd.melt(
            df_dados,
            id_vars=id_vars
            + (["estagio"] if "estagio" in df_dados.columns else []),
            value_vars=value_vars,
            var_name="conta",
            value_name="valor",
        )
        df_dados = df_dados.drop(to_drop, axis=1)

        # Fix known conta label inconsistency in 2004-2012 source files.
        if 2004 <= ano <= 2012:
            df_dados.loc[
                df_dados["conta"] == "ADAD  Inden e Restituições", "conta"
            ] = "ADAD Inden e Restituições"
            df_dados.loc[
                df_dados["conta"] == "IFAD  Inden e Restituições", "conta"
            ] = "IFAD Inden e Restituições"

        df_dados["ano"] = str(ano)

        # For 2009-2012, estagio is set on each row and the comp has separate rows
        # per estagio — merge on estagio too so each data row gets exactly one match.
        if 2009 <= ano <= 2012:
            chaves_legacy = ["ano", "estagio", "conta"]
        else:
            chaves_legacy = ["ano", "conta"]
        df_dados = df_dados.merge(
            df_comp_despesas, how="left", on=chaves_legacy
        )

        df_dados["conta"] = df_dados["conta"].astype("string")
        df_dados = df_dados.fillna("")
        df_dados["valor"] = pd.to_numeric(
            df_dados["valor"], errors="coerce"
        ).astype("float")
        df_dados = _apply_currency_conversion(df_dados, ano)
        parts.append(df_dados[ORDEM_MUNICIPIO])

    df = pd.concat(parts, ignore_index=True)
    print(
        f"  municipio_despesas_orcamentarias {ano}: {len(df):,} rows (legacy)"
    )
    partition_and_save(df, "municipio_despesas_orcamentarias", ano, path_dados)
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# API path (2013+)
# ---------------------------------------------------------------------------


def _build_api(path_dados, comp, year_data, ano):
    df_comp = comp["despesas"]
    df = year_data.get(LEVEL, {}).get(ANEXO)
    if df is None or df.empty:
        print(f"  municipio_despesas_orcamentarias {ano}: no data, skipping")
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
        f"  municipio_despesas_orcamentarias {ano}: {len(df):,} rows from {n} municipalities"
    )
    partition_and_save(df, "municipio_despesas_orcamentarias", ano, path_dados)
    return unmatched
