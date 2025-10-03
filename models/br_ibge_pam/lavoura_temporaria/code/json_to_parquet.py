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
        temp_variavel = j[0]["variavel"]
        for r in j[0]["resultados"]:
            temp_produto = list(r["classificacoes"][0]["categoria"].values())[  # noqa: RUF015
                0
            ]
            for s in r["series"]:
                temp_ano = list(s["serie"].keys())[0]  # noqa: RUF015
                temp_valor = list(s["serie"].values())[0]  # noqa: RUF015
                temp_sigla_uf = s["localidade"]["nome"].split("-")[-1].strip()
                temp_id_municipio = s["localidade"]["id"]

                temp_od["ano"] = temp_ano
                temp_od["sigla_uf"] = temp_sigla_uf
                temp_od["id_municipio"] = temp_id_municipio
                temp_od["produto"] = temp_produto
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


def drop_columns(dataframe):
    dropped_df = dataframe.drop(
        columns=[
            "Área plantada - percentual do total geral",
            "Área colhida - percentual do total geral",
            "Valor da produção - percentual do total geral",
        ]
    )

    return dropped_df


def rename_columns(dataframe):
    renamed_dataframe = dataframe.rename(
        columns={
            "Área plantada": "area_plantada",
            "Área colhida": "area_colhida",
            "Quantidade produzida": "quantidade_produzida",
            "Rendimento médio da produção": "rendimento_medio_producao",
            "Valor da produção": "valor_producao",
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
            "area_plantada",
            "area_colhida",
            "quantidade_produzida",
            "rendimento_medio_producao",
            "valor_producao",
        ]
    ]
    colunas_para_tratar = [
        "ano",
        "area_plantada",
        "area_colhida",
        "quantidade_produzida",
        "rendimento_medio_producao",
        "valor_producao",
    ]

    for coluna in colunas_para_tratar:
        dataframe[coluna] = dataframe[coluna].apply(
            lambda x: np.nan if x in ("-", "..", "...", "X") else x
        )
        dataframe[coluna] = dataframe[coluna].astype("Int64")
    return dataframe


def products_weight_ratio_fix(row):
    """
    2 - A partir do ano de 2001 as quantidades produzidas dos produtos abacate, banana,
    caqui, figo, goiaba, laranja, limão, maçã, mamão, manga, maracujá, marmelo, pera,
    pêssego e tangerina passam a ser expressas em toneladas. Nos anos anteriores eram
    expressas em mil frutos, com exceção da banana, que era expressa em mil cachos. O
    rendimento médio passa a ser expresso em Kg/ha. Nos anos anteriores era expresso
    em frutos/ha, com exceção da banana, que era expressa em cachos/ha.
    3 - Veja em o documento
    https://sidra.ibge.gov.br/content/documentos/pam/AlteracoesUnidadesMedidaFrutas.pdf
    com as alterações de unidades de medida das frutíferas ocorridas em 2001 e a tabela
    de conversão fruto x quilograma.
    """

    dicionario_de_proporcoes = {
        "Abacate": 0.38,
        "Banana (cacho)": 10.20,
        "Caqui": 0.18,
        "Figo": 0.09,
        "Goiaba": 0.16,
        "Larajna": 0.16,
        "Limão": 0.10,
        "Maçã": 0.15,
        "Mamão": 0.80,
        "Manga": 0.31,
        "Maracujá": 0.15,
        "Marmelo": 0.19,
        "Pera": 0.17,
        "Pêra": 0.17,  # Para garantir, pois nos dados parece que só há Pera, sem acento
        "Pêssego": 0.13,
        "Tangerina": 0.15,
        "Melancia": 6.08,
        "Melão": 1.39,
    }

    if row["ano"] >= 2001:
        return row

    if (
        pd.isna(row["quantidade_produzida"])
        or pd.isna(row["area_colhida"])
        or row["quantidade_produzida"] == 0
        or row["area_colhida"] == 0
    ):
        return row

    if row["produto"] not in dicionario_de_proporcoes:
        return row

    quantidade_produzida = (
        row["quantidade_produzida"] * dicionario_de_proporcoes[row["produto"]]
    )

    rendimento_medio_producao = (
        quantidade_produzida / row["area_colhida"] * 1000
    )  # kg / ha

    row["quantidade_produzida"] = quantidade_produzida
    row["rendimento_medio_producao"] = rendimento_medio_producao

    return row


def currency_fix(row):
    """
    Valor da produção (Mil Cruzeiros [1974 a 1985, 1990 a 1992], Mil Cruzados [1986 a 1988],
    Mil Cruzados Novos [1989], Mil Cruzeiros Reais [1993], Mil Reais [1994 a 2022])
    Verificado em http://www.igf.com.br/calculadoras/conversor/conversor.htm
    """

    if 1974 <= row["ano"] <= 1985:
        return row["valor_producao"] / (1000**4 * 2.75)
    elif 1986 <= row["ano"] <= 1988:
        return row["valor_producao"] / (1000**3 * 2.75)
    elif row["ano"] == 1989 or 1990 <= row["ano"] <= 1992:
        return row["valor_producao"] / (1000**2 * 2.75)
    elif row["ano"] == 1993:
        return row["valor_producao"] / (1000 * 2.75)
    else:
        return row["valor_producao"]


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
    ANOS_TRANSFORMADOS = get_existing_years("tmp/parquet")
    ARQUIVOS_JSON = list(Path("tmp/json/").glob("*.json"))
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
        print("Removendo as colunas do DataFrame...")
        df = drop_columns(df)
        print("Renomeando as colunas do DataFrame...")
        df = rename_columns(df)
        print("Tratando as colunas do DataFrame...")
        df = treat_columns(df)
        print(
            "Tratando a questão dos pesos dos produtos... Impacto em: quantidade_producao e rendimento_medio_producao"
        )
        df = df.apply(products_weight_ratio_fix, axis=1)
        print(
            "Aplicando a correção nominal retroativa da moeda... Impacto: valor_producao"
        )
        df["valor_producao"] = df.apply(currency_fix, axis=1)
        temp_ano = df["ano"].max()
        print("Deletando a coluna ano para possibilitar o particionamento...")
        df = df.drop(columns=["ano"])
        print("Transformações finalizadas!")
        temp_export_file_path = f"tmp/parquet/ano={temp_ano}/data.parquet"
        print(
            f"Exportando o DataFrame particionado em {temp_export_file_path}..."
        )
        os.makedirs(os.path.dirname(temp_export_file_path), exist_ok=True)
        temp_schema = pa.schema(
            [
                ("sigla_uf", pa.string()),
                ("id_municipio", pa.string()),
                ("produto", pa.string()),
                ("area_plantada", pa.int64()),
                ("area_colhida", pa.int64()),
                ("quantidade_produzida", pa.float64()),
                ("rendimento_medio_producao", pa.float64()),
                ("valor_producao", pa.float64()),
            ]
        )
        df.to_parquet(temp_export_file_path, schema=temp_schema, index=False)
        del df  # Liberando memória
        print()
