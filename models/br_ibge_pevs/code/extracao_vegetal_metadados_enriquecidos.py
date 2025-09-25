# -*- coding: utf-8 -*-
import os
from collections import OrderedDict

import pandas as pd
import requests

# Mudando o diretório de trabalho para o diretório do script
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

METADADOS_URL = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/289/metadados"
)


def get_metadados_categorias(row):
    codigo = row["codigo"]

    if codigo.count(".") == 0:
        row["tipo_produto"] = df_metadados.loc[
            df_metadados["codigo"] == codigo, "nome"
        ].iloc[0]
    else:
        row["produto"] = df_metadados.loc[
            df_metadados["codigo"] == codigo, "nome"
        ].iloc[0]
        if codigo.split(".")[0] in df_metadados["codigo"].tolist():
            row["tipo_produto"] = df_metadados.loc[
                df_metadados["codigo"] == codigo.split(".")[0], "nome"
            ].iloc[0]
    return row


def get_metadados_enriquecidos(df):
    df["tipo_produto"] = None
    df["produto"] = None

    df = df.apply(get_metadados_categorias, axis=1)
    return df


metadados_response_json = requests.get(METADADOS_URL).json()

classificacoes = metadados_response_json["classificacoes"][0]
categorias = classificacoes["categorias"]

metadados_dict_list = []

for categoria in categorias:
    if categoria["nome"] != "Total":
        temp_od = OrderedDict()
        temp_od["id"] = categoria["id"]
        temp_od["codigo"], temp_od["nome"] = categoria["nome"].split(" - ")

        metadados_dict_list.append(temp_od)

df_metadados = pd.DataFrame(metadados_dict_list)

df_metadados_enriquecidos = get_metadados_enriquecidos(df_metadados)
df_metadados_enriquecidos.to_csv(
    "extracao_vegetal_metadados_enriquecidos.csv", index=False
)
