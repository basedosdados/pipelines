# -*- coding: utf-8 -*-
import json
import os
import re
from collections import OrderedDict
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa


def parse_file(file_path):
    dict_list = []

    with open(file_path) as f:
        json_list = json.load(f)

    for j in json_list:
        temp_od = OrderedDict()
        temp_variavel = j["data"][0]["variavel"]
        for r in j["data"][0]["resultados"]:
            for s in r["series"]:
                temp_ano = list(s["serie"].keys())[0]
                temp_valor = list(s["serie"].values())[0]
                temp_sigla_uf = s["localidade"]["nome"].split("-")[-1].strip()
                temp_id_municipio = s["localidade"]["id"]

                temp_od["ano"] = temp_ano
                temp_od["sigla_uf"] = temp_sigla_uf
                temp_od["id_municipio"] = temp_id_municipio
                temp_od[temp_variavel] = temp_valor

                dict_list.append(dict(temp_od))
                temp_od.clear()
    return dict_list


def create_raw_df(dict_list):
    return pd.DataFrame(dict_list, dtype=str)


def rename_columns(dataframe):
    renamed_dataframe = dataframe.rename(
        columns={
            "Ovinos tosquiados nos estabelecimentos agropecu\u00e1rios": "ovinos_tosquiados"
        }
    )
    return renamed_dataframe


def treat_columns(dataframe):
    dataframe = dataframe[
        ["ano", "sigla_uf", "id_municipio", "ovinos_tosquiados"]
    ]
    COLUNAS_PARA_TRATAR = ["ovinos_tosquiados"]

    for coluna in COLUNAS_PARA_TRATAR:
        dataframe[coluna] = dataframe[coluna].apply(
            lambda x: np.nan if x in ("-", "..", "...", "X") else x
        )
        dataframe[coluna] = dataframe[coluna].astype("Int64")
    return dataframe


def get_existing_years(directory):
    root_dir = Path(directory)
    year_dirs = root_dir.glob("ano=*")
    years = set()
    for year_dir in year_dirs:
        match = re.match(r"ano=(\d+)", year_dir.name)
        if match:
            year = int(match.group(1))
            years.add(year)

    return sorted(list(years))


if __name__ == "__main__":
    ANOS_TRANSFORMADOS = get_existing_years("../parquet")
    ARQUIVOS_JSON = list(Path("../json/").glob("*.json"))
    JSON_FALTANTES = [
        arquivo
        for arquivo in ARQUIVOS_JSON
        if int(arquivo.stem) not in ANOS_TRANSFORMADOS
    ]

    for path in JSON_FALTANTES:
        print(f"Criando dict_list com base no arquivo: {path}...")
        dict_list = parse_file(path)
        print("Criando DataFrame a partir do dict_list...")
        df = create_raw_df(dict_list)
        print("Renomeando as colunas do DataFrame...")
        df = rename_columns(df)
        print("Tratando as colunas do DataFrame...")
        df = treat_columns(df)
        print("Transformações finalizadas!")
        temp_ano = df["ano"].max()
        print("Deletando a coluna ano para possibilitar o particionamento...")
        df.drop(columns=["ano"], inplace=True)
        print("Transformações finalizadas!")
        temp_export_file_path = f"../parquet/ano={temp_ano}/data.parquet"
        print(
            f"Exportando o DataFrame particionado em {temp_export_file_path}..."
        )
        os.makedirs(os.path.dirname(temp_export_file_path), exist_ok=True)
        temp_schema = pa.schema(
            [
                ("sigla_uf", pa.string()),
                ("id_municipio", pa.string()),
                ("ovinos_tosquiados", pa.int64()),
            ]
        )
        df.to_parquet(temp_export_file_path, schema=temp_schema, index=False)
        del df  # Liberando memória
        print()
