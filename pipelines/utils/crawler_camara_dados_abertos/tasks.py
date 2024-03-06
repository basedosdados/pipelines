# -*- coding: utf-8 -*-
# register tasks
import os
from datetime import timedelta
import pandas as pd
from prefect import task
import requests
from pipelines.constants import constants
from pipelines.utils.crawler_camara_dados_abertos.constants import (
    constants as constants_camara,
)
from pipelines.utils.crawler_camara_dados_abertos.utils import (
    download_and_read_data
)
from pipelines.utils.utils import log

# ----------------------------------------> DADOS CAMARA ABERTA - UNIVERSAL

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def save_data(table_id: str) -> str:
    df = download_and_read_data(table_id)
    if not os.path.exists(f'{constants_camara.OUTPUT_PATH.value}{table_id}'):
        os.makedirs(f'{constants_camara.OUTPUT_PATH.value}{table_id}')
    log(f'{constants_camara.OUTPUT_PATH.value}{table_id}')
    output_path = constants_camara.TABLES_OUTPUT_PATH.value[table_id]

    if table_id == "proposicao_microdados":
        df["ultimoStatus_despacho"] = df["ultimoStatus_despacho"].apply(
            lambda x: str(x).replace(";", ",").replace("\n", " ").replace("\r", " ")
        )
        df["ementa"] = df["ementa"].apply(
            lambda x: str(x).replace(";", ",").replace("\n", " ").replace("\r", " ")
        )
        df["ano"] = df.apply(
            lambda x: x["dataApresentacao"][0:4] if x["ano"] == 0 else x["ano"],
            axis=1,
        )

    if table_id == "frente_deputado":
        df = df.rename(columns=constants_camara.RENAME_COLUMNS_FRENTE_DEPUTADO.value)

    if table_id == "evento":
        df = df.rename(columns=constants_camara.RENAME_COLUMNS_EVENTO.value)
        df["descricao"] = df["descricao"].apply(
            lambda x: str(x).replace("\n", " ").replace("\r", " ")
        )

    if table_id == "votacao":
        df['ultimaApresentacaoProposicao_descricao'] = df['ultimaApresentacaoProposicao_descricao'].apply(lambda x: str(x).replace(';', ' ').replace("\n", " ").replace("\r", " "))

    if table_id == "votacao_objeto":
        df[['descricao', "proposicao_ementa"]] = df[['descricao', "proposicao_ementa"]].apply(lambda x: str(x).replace(';', ' ').replace("\n", " ").replace("\r", " "))

    if table_id == "votacao_proposicao":
        df[["proposicao_ementa"]] = df[["proposicao_ementa"]].apply(lambda x: str(x).replace(';', ' ').replace("\n", " ").replace("\r", " "))

    if table_id == "licitacao_pedido":
        df[["observacoes"]] = df[["observacoes"]].apply(lambda x: str(x).replace(';', ' ').replace("\n", " ").replace("\r", " "))

    if table_id == "licitacao_item":
        df[["especificacao"]] = df[["especificacao"]].apply(lambda x: str(x).replace(';', ' ').replace("\n", " ").replace("\r", " "))

    log(f"Saving {table_id} to {output_path}")

    df.to_csv(output_path, sep=",", index=False, encoding="utf-8")

    return output_path

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def check_if_url_is_valid(table_id:str) -> bool:
    if requests.get(constants_camara.TABLES_URL.value[table_id]).status_code == 200:
        log("URL is valid")
        return True
    else:
        log("URL is not valid")
        return False

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def dict_update_django_metadata(table_id: str, dataset_id = "br_camara_dados_abertos"):
    dict_of_table = {
                                "deputado": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "deputado_ocupacao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "deputado_profissao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "evento": {
                                    "dataset_id":dataset_id,
                                    "table_id":table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "coverage_type":"part_bdpro",
                                    "time_delta":{'months': 6},
                                    "prefect_mode":"prod",
                                    "bq_project":"basedosdados",
                                    "historical_database":True
                                },
                                "evento_orgao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "evento_presenca_deputado": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "evento_requerimento": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "frente": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_criacao'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "frente_deputado": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "funcionario": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_inicio_historico'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "licitacao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_abertura'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "licitacao_item": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "licitacao_contrato": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_assinatura'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "licitacao_pedido": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_cadastro'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "licitacao_proposta": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "orgao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "orgao_deputado": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "proposicao_autor": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "proposicao_micrdados": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "proposicao_tema": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "votacao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "votacao_objeto": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "votacao_parlamentar ": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "votacao_proposicao ": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "votacao_orientacao_bancada": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                }
                                }
    return dict_of_table.get(table_id)