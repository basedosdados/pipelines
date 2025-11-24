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
        temp_unidade = j["data"][0]["unidade"]
        for r in j["data"][0]["resultados"]:
            temp_caracteristica = list(  # noqa: RUF015
                r["classificacoes"][0]["categoria"].values()
            )[0]
            for s in r["series"]:
                temp_ano = list(s["serie"].keys())[0]  # noqa: RUF015
                temp_valor = list(s["serie"].values())[0]  # noqa: RUF015
                temp_sigla_uf = s["localidade"]["nome"].split("-")[-1].strip()
                temp_id_municipio = s["localidade"]["id"]

                temp_od["ano"] = temp_ano
                temp_od["sigla_uf"] = temp_sigla_uf
                temp_od["id_municipio"] = temp_id_municipio
                temp_od["produto"] = temp_caracteristica
                if temp_unidade not in [
                    "Mil Cruzeiros",
                    "Mil Cruzados",
                    "Mil Cruzados Novos",
                    "Mil Cruzeiros Reais",
                    "Mil Reais",
                ]:
                    temp_od["unidade"] = temp_unidade
                temp_od[temp_variavel] = temp_valor

                dict_list.append(dict(temp_od))
                temp_od.clear()
    return dict_list


def join_dicts_by_keys(dict_list, keys):
    result = {}
    for d in dict_list:
        key_tuple = tuple(d[k] for k in keys)
        if key_tuple not in result:
            result[key_tuple] = d
        else:
            result[key_tuple].update(d)
    return list(result.values())


def create_raw_df(dict_list):
    return pd.DataFrame(dict_list, dtype=str)


def rename_columns(dataframe):
    renamed_dataframe = dataframe.rename(
        columns={
            "Produção de origem animal": "quantidade",
            "Valor da produção": "valor",
        }
    )
    return renamed_dataframe


def treat_columns(dataframe):
    dataframe = dataframe[
        [
            "ano",
            "sigla_uf",
            "id_municipio",
            "produto",
            "unidade",
            "quantidade",
            "valor",
        ]
    ]
    colunas_para_tratar = ["ano", "quantidade", "valor"]

    for coluna in colunas_para_tratar:
        dataframe[coluna] = dataframe[coluna].apply(
            lambda x: np.nan if x in ("-", "..", "...", "X") else x
        )
        dataframe[coluna] = dataframe[coluna].astype("Int64")
    return dataframe


def currency_fix(row):
    """
    Valor da produção (Mil Cruzeiros [1974 a 1985, 1990 a 1992], Mil Cruzados [1986 a 1988],
    Mil Cruzados Novos [1989], Mil Cruzeiros Reais [1993], Mil Reais [1994 a 2022])
    Verificado em http://www.igf.com.br/calculadoras/conversor/conversor.htm
    """

    if 1974 <= row["ano"] <= 1985:
        return row["valor"] / (1000**4 * 2.75)
    elif 1986 <= row["ano"] <= 1988:
        return row["valor"] / (1000**3 * 2.75)
    elif row["ano"] == 1989 or 1990 <= row["ano"] <= 1992:
        return row["valor"] / (1000**2 * 2.75)
    elif row["ano"] == 1993:
        return row["valor"] / (1000 * 2.75)
    else:
        return row["valor"]


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
    ANOS_TRANSFORMADOS = get_existing_years(
        f"{Path.cwd()}/output/producao_origem_animal/parquet"
    )
    ARQUIVOS_JSON = list(
        Path(f"{Path.cwd()}/output/producao_origem_animal/json/").glob(
            "*.json"
        )
    )
    JSON_FALTANTES = [
        arquivo
        for arquivo in ARQUIVOS_JSON
        if int(arquivo.stem) not in ANOS_TRANSFORMADOS
    ]

    for path in JSON_FALTANTES:
        print(f"Criando dict_list com base no arquivo: {path}...")
        dict_list = parse_file(path)
        print("Concatendo as colunas do dict_list...")
        dict_list = join_dicts_by_keys(
            dict_list, ["ano", "sigla_uf", "id_municipio", "produto"]
        )
        print("Criando DataFrame a partir do dict_list...")
        df = create_raw_df(dict_list)
        print("Renomeando as colunas do DataFrame...")
        df = rename_columns(df)
        print("Tratando as colunas do DataFrame...")
        df = treat_columns(df)
        print(
            "Aplicando a correção nominal retroativa da moeda... Impacto: valor"
        )
        df["valor"] = df.apply(currency_fix, axis=1)
        print("Transformações finalizadas!")
        temp_ano = df["ano"].max()
        print("Deletando a coluna ano para possibilitar o particionamento...")
        df = df.drop(columns=["ano"])
        print("Transformações finalizadas!")
        temp_export_file_path = f"{Path.cwd()}/output/producao_origem_animal/parquet/ano={temp_ano}/data.parquet"
        print(
            f"Exportando o DataFrame particionado em {temp_export_file_path}..."
        )
        os.makedirs(os.path.dirname(temp_export_file_path), exist_ok=True)
        temp_schema = pa.schema(
            [
                ("sigla_uf", pa.string()),
                ("id_municipio", pa.string()),
                ("produto", pa.string()),
                ("unidade", pa.string()),
                ("quantidade", pa.int64()),
                ("valor", pa.int64()),
            ]
        )
        df.to_parquet(temp_export_file_path, schema=temp_schema, index=False)
        del df  # Liberando memória
        print()
