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
    verify_total,
)
from br_denatran_frota.code.constants import DICT_UFS, SUBSTITUTIONS

## TODO: Extract everything then run this? Create prefect? where to go now

import basedosdados as bd


def verify_uf(denatran_df: pl.DataFrame, ibge_df: pl.DataFrame, uf: str) -> None:
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
    d = set(municipios_no_denatran) - set(municipios_na_bd)
    municipios_duplicados = (
        denatran_uf.groupby("suggested_nome_ibge").count().filter(pl.col("count") > 1)
    )
    if not municipios_duplicados.is_empty():
        raise ValueError(
            f"Existem municípios com mesmo nome do IBGE em {uf}! São eles {municipios_duplicados['suggested_nome_ibge'].to_list()}"
        )
    if d:
        # This here is probably impossible and shouldn't happen due to the matching coming from the BD data.
        # The set difference might occur the other way around, but still, better safe.
        raise ValueError(f"Existem municípios em {uf} que não estão na BD.")
    match_ibge(denatran_uf, ibge_uf)


municipios_query = """SELECT nome, id_municipio, sigla_uf FROM `basedosdados.br_bd_diretorios_brasil.municipio`
"""
bd_municipios = bd.read_sql(municipios_query, "tamir-pipelines")
bd_municipios = pl.from_pandas(bd_municipios)

file = "br_denatran_frota/files/2022/frota_por_município_e_tipo_2-2022.xls"  # Hardcode problem
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
if new_df.shape[0] > bd_municipios.shape[0]:
    raise ValueError(
        f"Atenção: a base do Denatran tem {new_df.shape[0]} linhas e isso é mais municípios do que a BD com {bd_municipios.shape[0]}"
    )

verify_total(new_df)
for uf in DICT_UFS:
    verify_uf(new_df, bd_municipios, uf)
