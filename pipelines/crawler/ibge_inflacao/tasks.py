"""
Tasks for br_ibge_inpc
"""

import asyncio
import os
import ssl
from datetime import datetime
from datetime import datetime as dt
from time import sleep

import basedosdados as bd
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from prefect import task
from tqdm import tqdm

from pipelines.crawler.ibge_inflacao.constants import constants
from pipelines.crawler.ibge_inflacao.utils import (
    collect_data,
    get_legacy_session,
    json_categoria,
    json_mes_brasil,
)
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url
from pipelines.utils.utils import log, to_partitions

# necessary for use wget, see: https://stackoverflow.com/questions/35569042/ssl-certificate-verify-failed-with-python3
ssl._create_default_https_context = ssl._create_unverified_context
# pylint: disable=C0206
# pylint: disable=C0201
# pylint: disable=R0914
# https://sidra.ibge.gov.br/tabela/7062
# https://sidra.ibge.gov.br/tabela/7063
# https://sidra.ibge.gov.br/tabela/7060


@task
def check_for_updates(
    table_id: str,
    dataset_id: str,
) -> bool:
    """
    Crawler para checar atualizações nas dos conjuntos br_ibge_inpc; br_ibge_ipca; br_ibge_ipca15

    indice: inpc | ipca | ip15
    """

    n_mes = {
        "janeiro": "1",
        "fevereiro": "2",
        "março": "3",
        "abril": "4",
        "maio": "5",
        "junho": "6",
        "julho": "7",
        "agosto": "8",
        "setembro": "9",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12",
    }

    if dataset_id not in ["br_ibge_inpc", "br_ibge_ipca", "br_ibge_ipca15"]:
        raise ValueError(
            "indice argument must be one of the following: 'inpc', 'ipca', 'ipca15'"
        )

    log(
        f"Checking for updates in {dataset_id} index for {dataset_id}.{table_id}"
    )

    links = {
        "ipca": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n1/all/v/all/p/last%201/c315/7169/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "inpc": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n1/all/v/all/p/last%201/c315/7169/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "ip15": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n1/all/v/all/p/last%201/c315/7169/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
    }

    links = {k: v for k, v in links.items() if k.__contains__(table_id)}

    links_keys = list(links.keys())
    log(links_keys)
    success_dwnl = []

    os.system('mkdir -p "/tmp/check_for_updates/"')
    #
    for key in tqdm(links_keys):
        try:
            response = get_legacy_session().get(links[key])
            # download the csv
            with open(f"/tmp/check_for_updates/{key}.csv", "wb") as f:
                f.write(response.content)
            success_dwnl.append(key)
            sleep(5)
        except Exception as e:
            log(e)
            try:
                sleep(5)
                response = get_legacy_session().get(links[key])
                # download the csv
                with open(f"/tmp/check_for_updates/{key}.csv", "wb") as f:
                    f.write(response.content)
                success_dwnl.append(key)
            except Exception as e:  # pylint: disable=redefined-outer-name
                log(e)

    log(f"success_dwnl: {success_dwnl}")
    if len(links_keys) == len(success_dwnl):
        log("All files were successfully downloaded")

    # quebra o flow se houver erro no download de um arquivo.
    else:
        rems = set(links_keys) - set(success_dwnl)
        log(f"The file was not downloaded {rems}")

    file_name = os.listdir("/tmp/check_for_updates")
    file_path = "/tmp/check_for_updates/" + file_name[0]

    dataframe = pd.read_csv(file_path, skiprows=2, skipfooter=14, sep=";")

    dataframe = dataframe[["Mês"]]

    dataframe[["mes", "ano"]] = dataframe["Mês"].str.split(
        pat=" ", n=1, expand=True
    )

    dataframe["mes"] = dataframe["mes"].map(n_mes)

    dataframe = dataframe["ano"][0] + "-" + dataframe["mes"][0]

    dataframe = dt.strptime(dataframe, "%Y-%m")

    max_date_ibge = dataframe.strftime("%Y-%m")

    log(
        f"A data mais no site do ---IBGE--- para a tabela {table_id} é : {dataframe.date()}"
    )
    backend = bd.Backend(graphql_url=get_url("prod"))
    max_date_bd = get_api_most_recent_date(
        table_id=table_id,
        dataset_id=dataset_id,
        date_format="%Y-%m",
        backend=backend,
    )
    log(
        f"A data mais recente da tabela no --- Site da BD --- é: {max_date_bd}"
    )
    if dataframe.date() > max_date_bd:
        log(
            f"A tabela {table_id} foi atualizada no site do IBGE. O Flow de atualização será executado!"
        )

        data_base = datetime.strptime(max_date_ibge, "%Y-%m").date()

        data_proxima = data_base + relativedelta(months=+1)
        ano = str(data_proxima.year)
        mes = f"{data_proxima.month:02d}"

        ano_mes = ano + mes

        return True, str(ano_mes)
    else:
        log(
            f"A tabela {table_id} não foi atualizada no site do IBGE. O Flow de atualização não será executado!"
        )
        return False, str(max_date_ibge)


@task
def collect_data_utils(dataset_id: str, table_id: str, periodo: str) -> None:
    if table_id == "mes_brasil":
        asyncio.run(
            collect_data(
                dataset_id=dataset_id,
                table_id=table_id,
                aggregates=constants.DATASETS_MES_BRASIL.value[dataset_id][
                    "aggregates"
                ],
                variables=constants.DATASETS_MES_BRASIL.value[dataset_id][
                    "variables"
                ],
                periods=periodo,
                geo_level=constants.GEO_LEVELS_MES_BRASIL.value,
            )
        )
    else:
        asyncio.run(
            collect_data(
                dataset_id=dataset_id,
                table_id=table_id,
                aggregates=constants.DATASETS.value[dataset_id]["aggregates"],
                variables=constants.DATASETS.value[dataset_id]["variables"],
                periods=periodo,
                geo_level=constants.GEO_LEVELS.value[table_id],
            )
        )


@task
def json_to_csv(table_id: str, dataset_id: str, periodo: str):
    if table_id == "mes_brasil":
        dados_agrupados = json_mes_brasil(
            dataset_id=dataset_id, table_id=table_id, periodo=periodo
        )
    else:
        dados_agrupados = json_categoria(
            dataset_id=dataset_id, table_id=table_id, periodo=periodo
        )

    df = pd.DataFrame(list(dados_agrupados.values()))
    df = df.apply(lambda x: x.replace("-", np.nan))

    output_path = os.path.join(constants.OUTPUT.value, dataset_id, table_id)
    to_partitions(
        data=df,
        savepath=output_path,
        partition_columns=["ano", "mes"],
    )

    return output_path
