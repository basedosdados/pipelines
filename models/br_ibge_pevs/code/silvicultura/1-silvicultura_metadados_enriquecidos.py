from collections import OrderedDict

import pandas as pd
import requests

METADADOS_URL = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/291/metadados"
)


def get_metadados_categorias(row):
    codigo = row["codigo"]

    if codigo.startswith("2"):
        row["categoria_produto"] = df_metadados.loc[
            df_metadados["codigo"] == codigo.split(".")[0], "nome"
        ].iloc[0]
        if codigo.count(".") > 0:
            row["produto"] = df_metadados.loc[
                df_metadados["codigo"] == codigo, "nome"
            ].iloc[0]
    else:
        if codigo.startswith("1.3"):
            row["tipo_produto"] = df_metadados.loc[
                df_metadados["codigo"] == "1.3", "nome"
            ].iloc[0]
            if codigo.count(".") > 1:
                row["subtipo_produto"] = df_metadados.loc[
                    df_metadados["codigo"] == ".".join(codigo.split(".")[:3]),
                    "nome",
                ].iloc[0]
                if codigo.count(".") > 2:
                    row["produto"] = df_metadados.loc[
                        df_metadados["codigo"] == codigo, "nome"
                    ].iloc[0]
        elif codigo.startswith("1.2"):
            row["tipo_produto"] = df_metadados.loc[
                df_metadados["codigo"] == "1.2", "nome"
            ].iloc[0]
            if codigo.count(".") > 1:
                row["produto"] = df_metadados.loc[
                    df_metadados["codigo"] == codigo, "nome"
                ].iloc[0]
        elif codigo.startswith("1.1"):
            row["tipo_produto"] = df_metadados.loc[
                df_metadados["codigo"] == "1.1", "nome"
            ].iloc[0]
            if codigo.count(".") > 1:
                row["produto"] = df_metadados.loc[
                    df_metadados["codigo"] == codigo, "nome"
                ].iloc[0]
    return row


def get_metadados_enriquecidos(df):
    df["categoria_produto"] = None
    df["tipo_produto"] = None
    df["subtipo_produto"] = None
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
    "silvicultura_metadados_enriquecidos.csv", index=False
)
