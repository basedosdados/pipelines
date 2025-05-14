# -*- coding: utf-8 -*-
from pathlib import Path

import numpy as np
import pandas as pd


def to_partitions(
    data: pd.DataFrame, partition_columns: list[str], savepath: str
):
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """

    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)

        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            file_filter_save_path = Path(filter_save_path) / "data.csv"

            # append data to csv
            df_filter.to_csv(
                file_filter_save_path,
                index=False,
                mode="a",
                header=not file_filter_save_path.exists(),
            )
    else:
        raise BaseException("Data need to be a pandas DataFrame")


# Loop para cada ano de 1995 a 2023
for anos in range(1995, 2024):
    # Lendo o arquivo CSV usando pandas
    df = pd.read_csv(
        f"D:\download\iptu\EG_{anos}.csv", sep=";", encoding="utf-8"
    )

    # Dicionário de renomeação de colunas
    rename = {
        "NUMERO DO CONTRIBUINTE": "numero_contribuinte",
        "ANO DO EXERCICIO": "ano",
        "NUMERO DA NL": "numero_notificacao",
        "DATA DO CADASTRAMENTO": "data_cadastramento",
        "NUMERO DO CONDOMINIO": "numero_condominio",
        "CODLOG DO IMOVEL": "codigo_logradouro",
        "NOME DE LOGRADOURO DO IMOVEL": "logradouro",
        "NUMERO DO IMOVEL": "numero_imovel",
        "COMPLEMENTO DO IMOVEL": "complemento",
        "BAIRRO DO IMOVEL": "bairro",
        "REFERENCIA DO IMOVEL": "referencia_imovel",
        "CEP DO IMOVEL": "cep",
        "TIPO DE USO DO IMOVEL": "finalidade_imovel",
        "TIPO DE PADRAO DA CONSTRUCAO": "tipo_construcao",
        "TIPO DE TERRENO": "tipo_terreno",
        "QUANTIDADE DE ESQUINAS FRENTES": "quantidade_esquina_imovel",
        "FRACAO IDEAL": "fracao_ideal",
        "AREA DO TERRENO": "area_terreno",
        "AREA CONSTRUIDA": "area_construida",
        "AREA OCUPADA": "area_ocupada",
        "VALOR DO M2 DO TERRENO": "valor_terreno",
        "VALOR DO M2 DE CONSTRUCAO": "valor_construcao",
        "ANO DA CONSTRUCAO CORRIGIDO": "ano_construcao_corrigida",
        "QUANTIDADE DE PAVIMENTOS": "quantidade_pavimento",
        "TESTADA PARA CALCULO": "testada_imovel",
        "ANO DE INICIO DA VIDA DO CONTRIBUINTE": "ano_inicio_vida_contribuinte",
        "MES DE INICIO DA VIDA DO CONTRIBUINTE": "mes_inicio_vida_contribuinte",
        "FASE DO CONTRIBUINTE": "fase_contribuinte",
        "FATOR DE OBSOLESCENCIA": "fator_obsolescencia",
    }

    # Lista de colunas na ordem desejada
    ordem = [
        "ano",
        "data_cadastramento",
        "numero_contribuinte",
        "numero_notificacao",
        "numero_condominio",
        "codigo_logradouro",
        "logradouro",
        "numero_imovel",
        "cep",
        "complemento",
        "bairro",
        "referencia_imovel",
        "finalidade_imovel",
        "tipo_construcao",
        "tipo_terreno",
        "fracao_ideal",
        "area_terreno",
        "area_construida",
        "area_ocupada",
        "testada_imovel",
        "quantidade_esquina_imovel",
        "valor_terreno",
        "valor_construcao",
        "ano_construcao_corrigida",
        "quantidade_pavimento",
        "ano_inicio_vida_contribuinte",
        "mes_inicio_vida_contribuinte",
        "fase_contribuinte",
        "fator_obsolescencia",
    ]

    df.rename(columns=rename, inplace=True)

    # Substituir vírgulas por pontos em colunas específicas
    colunas_virgula = [
        "fator_obsolescencia",
        "testada_imovel",
        "valor_construcao",
        "valor_terreno",
        "fracao_ideal",
    ]

    for coluna_virgula in colunas_virgula:
        df[coluna_virgula] = df[coluna_virgula].apply(
            lambda x: str(x).replace(",", ".")
        )

    # Remover traços em colunas específicas
    colunas_traco = ["cep", "numero_contribuinte"]
    for coluna_traco in colunas_traco:
        df[coluna_traco] = df[coluna_traco].apply(
            lambda x: str(x).replace("-", "")
        )

    # Remover ".0" em colunas específicas
    colunas_ponto_zero = [
        "ano_inicio_vida_contribuinte",
        "mes_inicio_vida_contribuinte",
        "numero_imovel",
        "quantidade_esquina_imovel",
        "area_terreno",
        "area_construida",
        "area_ocupada",
        "ano_construcao_corrigida",
        "quantidade_pavimento",
        "valor_terreno",
        "cep",
        "valor_construcao",
    ]

    for coluna_ponto_zero in colunas_ponto_zero:
        df[coluna_ponto_zero] = df[coluna_ponto_zero].apply(
            lambda x: str(x).replace(".0", "")
        )

    # Formatando as datas para o padrão YYYY-MM-DD nos arquivos de 2016 a 2023
    # Em anos anteriores a 2016, as datas estão vazias
    if (df["ano"] <= 2015).all():
        df["data_cadastramento"] = np.nan
    else:

        def formatar_data(data):
            dia = data[:2]
            mes = data[3:5]
            ano = data[6:]
            return f"20{ano}-{mes}-{dia}"

        df["data_cadastramento"] = df["data_cadastramento"].apply(
            formatar_data
        )

    df = df.apply(lambda x: x.replace(np.nan, "").replace(",", "."))

    # Ordenando as colunas
    df = df[ordem]

    # Particionando o DataFrame por ano a partir da função acima.
    to_partitions(
        df, partition_columns=["ano"], savepath="D:\download\iptu\output"
    )
