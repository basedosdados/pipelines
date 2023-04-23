# -*- coding: utf-8 -*-
import polars as pl
import pandas as pd
from string_utils import asciify
import os
from br_denatran_frota.code.utils import (
    change_df_header,
    fix_suggested_nome_ibge,
    guess_header,
    get_city_name_ibge,
    match_ibge,
)
from br_denatran_frota.code.constants import DICT_UFS, SUBSTITUTIONS

import basedosdados as bd

municipios_query = """SELECT nome, id_municipio, sigla_uf FROM `basedosdados.br_bd_diretorios_brasil.municipio`
"""
bd_municipios = bd.read_sql(municipios_query, "tamir-pipelines")
bd_municipios = pl.from_pandas(bd_municipios)

file = "br_denatran_frota/files/2022/frota_por_município_e_tipo_2-2022.xls"
filename = os.path.split(file)[1]
df = pd.read_excel(file)
new_df = change_df_header(df, guess_header(df))
new_df.rename(
    columns={new_df.columns[0]: "sigla_uf", new_df.columns[1]: "nome_denatran"},
    inplace=True,
)  # Rename for ease of use.
new_df.sigla_uf = new_df.sigla_uf.str.strip()  # Remove whitespace.
new_df = pl.from_pandas(new_df)
new_df = new_df.with_columns(pl.col("nome_denatran").apply(asciify).str.to_lowercase())
new_df = new_df.filter(pl.col("nome_denatran") != "municipio nao informado")


def verify_uf(denatran_df: pl.DataFrame, ibge_df: pl.DataFrame, uf: str):
    denatran_uf = denatran_df.filter(pl.col("sigla_uf") == uf)
    ibge_uf = ibge_df.filter(pl.col("sigla_uf") == uf)
    ibge_uf = ibge_uf.with_columns(pl.col("nome").apply(asciify).str.to_lowercase())
    municipios_na_bd = ibge_uf["nome"].to_list()
    x = denatran_uf["nome_denatran"].apply(lambda x: get_city_name_ibge(x, ibge_uf))
    denatran_uf = denatran_uf.with_columns(x.alias("suggested_nome_ibge"))
    denatran_uf = denatran_uf.with_columns(
        denatran_uf.apply(fix_suggested_nome_ibge)["apply"].alias("suggested_nome_ibge")
    )
    municipios_no_denatran = denatran_uf["suggested_nome_ibge"].to_list()
    d = set(municipios_na_bd) - set(municipios_no_denatran)
    z = denatran_uf.groupby("suggested_nome_ibge").count().filter(pl.col("count") > 1)
    if not z.is_empty():
        print(z["suggested_nome_ibge"].to_list(), uf)
    if d:
        print(d, uf)
    match_ibge(denatran_uf, ibge_uf)


for uf in DICT_UFS:
    verify_uf(new_df, bd_municipios, uf)
