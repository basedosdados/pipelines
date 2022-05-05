# -*- coding: utf-8 -*-
"""
Tasks for br_poder360_pesquisas
"""
from json.decoder import JSONDecodeError
from datetime import timedelta
import os

from prefect import task
import requests
import pandas as pd
from tqdm import tqdm
from pipelines.constants import constants

# pylint: disable=C0103
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler() -> str:
    """
    Crawler to consume poder360's API  and save local csv
    """
    os.system("mkdir -p /tmp/data/poder360/")
    header = [
        "id_pesquisa",
        "ano",
        "sigla_uf",
        "nome_municipio",
        "cargo",
        "data",
        "data_referencia",
        "instituto",
        "contratante",
        "orgao_registro",
        "numero_registro",
        "quantidade_entrevistas",
        "margem_mais",
        "margem_menos",
        "tipo",
        "turno",
        "tipo_voto",
        "id_cenario",
        "descricao_cenario",
        "id_candidato_poder360",
        "nome_candidato",
        "sigla_partido",
        "condicao",
        "percentual",
    ]

    data = pd.DataFrame(columns=header)

    for year in tqdm(range(2000, 2023)):
        url = f"https://pesquisas.poder360.com.br/web/consulta/fetch?data_pesquisa_de={year}-01-01&data_pesquisa_ate={year}-12-31&order_column=ano&order_type=asc"
        response = requests.get(url, headers={"User-Agent": "Magic Browser"})
        try:
            data_json = response.json()
        except (JSONDecodeError, ValueError):
            continue
        df = pd.json_normalize(data_json)
        if df.shape[0] > 0:
            df = df[
                [
                    "pesquisa_id",
                    "ano",
                    "ambito",
                    "cargo",
                    "tipo",
                    "turno",
                    "data_pesquisa",
                    "instituto",
                    "voto_tipo",
                    "cenario_id",
                    "cenario_descricao",
                    "candidatos_id",
                    "candidato",
                    "condicao",
                    "percentual",
                    "data_referencia",
                    "margem_mais",
                    "margem_menos",
                    "contratante",
                    "num_registro",
                    "orgao_registro",
                    "qtd_entrevistas",
                    "partido",
                    "cidade",
                ]
            ]
            df.columns = [
                "id_pesquisa",
                "ano",
                "sigla_uf",
                "cargo",
                "tipo",
                "turno",
                "data",
                "instituto",
                "tipo_voto",
                "id_cenario",
                "descricao_cenario",
                "id_candidato_poder360",
                "nome_candidato",
                "condicao",
                "percentual",
                "data_referencia",
                "margem_mais",
                "margem_menos",
                "contratante",
                "numero_registro",
                "orgao_registro",
                "quantidade_entrevistas",
                "sigla_partido",
                "nome_municipio",
            ]
            df = df[header]
            df["sigla_uf"] = df["sigla_uf"].str.replace("BR", "")
            df["cargo"] = df["cargo"].str.lower()
            df["tipo"] = df["tipo"].str.lower()
            df["tipo_voto"] = df["tipo_voto"].str.lower()
            df["sigla_uf"] = df["sigla_uf"].str.replace("Novo", "NOVO")
            df["sigla_uf"] = df["sigla_uf"].str.replace("Patriota", "PATRI")
            df["sigla_uf"] = df["sigla_uf"].str.replace("Podemos", "PODE")
            df["sigla_uf"] = df["sigla_uf"].str.replace("Progressistas", "PP")
            df["sigla_uf"] = df["sigla_uf"].str.replace("Prona", "PRONA")
            df["sigla_uf"] = df["sigla_uf"].str.replace("Pros", "PROS")
            df["sigla_uf"] = df["sigla_uf"].str.replace("Psol", "PSOL")
            df["sigla_uf"] = df["sigla_uf"].str.replace("Rede", "REDE")

            data = pd.concat([data, df])
        else:
            continue

    filepath = "/tmp/data/poder360/microdados_poder360.csv"
    data.to_csv(filepath, index=False)
    return filepath
