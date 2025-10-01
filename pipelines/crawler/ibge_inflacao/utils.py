import asyncio
import json
import os
import ssl
from collections import defaultdict
from datetime import datetime
from datetime import datetime as dt
from time import sleep
from typing import Any

import aiohttp
import basedosdados as bd
import numpy as np
import pandas as pd
import requests
import urllib3
from aiohttp import ClientTimeout, TCPConnector
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from tqdm.asyncio import tqdm as tqdm_asyncio

from pipelines.crawler.ibge_inflacao.constants import constants
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url
from pipelines.utils.utils import log

# necessary for use wget, see: https://stackoverflow.com/questions/35569042/ssl-certificate-verify-failed-with-python3
ssl._create_default_https_context = ssl._create_unverified_context
# pylint: disable=C0206
# pylint: disable=C0201
# pylint: disable=R0914
# https://sidra.ibge.gov.br/tabela/7062
# https://sidra.ibge.gov.br/tabela/7063
# https://sidra.ibge.gov.br/tabela/7060


def build_url(
    aggregate: int, period: str, variable: int, geo_level: str, table_id: str
) -> str:
    """Monta a URL de consulta da API IBGE."""
    if table_id == "mes_brasil":
        return (
            f"https://servicodados.ibge.gov.br/api/v3/agregados/{aggregate}"
            f"/periodos/{period}/variaveis/{variable}"
            f"?localidades={geo_level}"
        )
    else:
        return (
            f"https://servicodados.ibge.gov.br/api/v3/agregados/{aggregate}"
            f"/periodos/{period}/variaveis/{variable}"
            f"?localidades={geo_level}&classificacao=315[all]"
        )


async def fetch(session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
    """Executa requisição GET e retorna a resposta em JSON."""
    async with session.get(url) as response:
        return await response.json()


def save_json(
    data: list[dict[str, Any]],
    dataset_id: str,
    table_id: str,
    period: str,
    output_dir: str,
) -> None:
    """Salva os dados em arquivo JSON no diretório definido."""
    output_path = os.path.join(output_dir, dataset_id, table_id, period)
    if os.path.exists(output_path):
        pass
        log("O Arquivo já existe!")
    else:
        os.makedirs(output_path, exist_ok=True)
        log("Criando o arquivo!")

        file_path = f"{output_path}/data.json"
        with open(file_path, "a") as f:
            json.dump(data, f)


async def collect_data(
    dataset_id: str,
    table_id: str,
    aggregates: list[int],
    variables: list[int],
    periods: list[str],
    geo_level: str,
) -> None:
    """Consulta dados da API IBGE e salva os resultados em JSON."""
    async with aiohttp.ClientSession(
        connector=TCPConnector(limit=100, force_close=True),
        timeout=ClientTimeout(total=1200),
    ) as session:
        for period in [periods]:
            log(
                f"Consultando período {period} | tabela: {table_id} | dataset_id: {dataset_id}"
            )
            for aggregate in aggregates:
                for variable in variables:
                    log(
                        build_url(
                            aggregate, period, variable, geo_level, table_id
                        )
                    )

            tasks = [
                fetch(
                    session,
                    build_url(
                        aggregate, period, variable, geo_level, table_id
                    ),
                )
                for aggregate in aggregates
                for variable in variables
            ]

            results = []
            for result in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
                try:
                    results.append(await result)
                except asyncio.TimeoutError:
                    print("⚠️ Timeout em uma requisição")

            save_json(
                data=results,
                dataset_id=dataset_id,
                table_id=table_id,
                period=period,
                output_dir=os.path.join(os.getcwd(), constants.INPUT.value),
            )


def json_categoria(table_id: str, dataset_id: str, periodo: str):
    input = os.path.join(
        constants.INPUT.value, dataset_id, table_id, periodo, "data.json"
    )
    with open(input) as f:
        df = json.load(f)

        log(df)

    # dicionário auxiliar para juntar na mesma linha
    dados_agrupados = defaultdict(dict)

    for indice_bloco in range(len(df)):
        total_resultados = len(df[indice_bloco][0]["resultados"])
        for indice_resultado in range(total_resultados):
            total_series = len(
                df[indice_bloco][0]["resultados"][indice_resultado]["series"]
            )
            for indice_serie in range(total_series):
                chave_serie = list(  # noqa: RUF015
                    df[indice_bloco][0]["resultados"][indice_resultado][
                        "series"
                    ][indice_serie]["serie"].keys()
                )[0]
                ano = chave_serie[0:4]
                mes = chave_serie[4:6]

                categoria_valor = list(  # noqa: RUF015
                    df[indice_bloco][0]["resultados"][indice_resultado][
                        "classificacoes"
                    ][0]["categoria"].values()
                )[0]
                if categoria_valor == "Índice geral":
                    categoria = "Índice geral"
                    id_categoria = np.nan
                else:
                    id_categoria, categoria = categoria_valor.split(".")

                if table_id == "mes_categoria_municipio":
                    localidade = df[indice_bloco][0]["resultados"][
                        indice_resultado
                    ]["series"][indice_serie]["localidade"]["nome"]
                    municipio = localidade.split(" - ")[0]
                    sigla_uf = localidade.split(" - ")[1]
                    chave_unica = (
                        ano,
                        mes,
                        id_categoria,
                        categoria,
                        municipio,
                        sigla_uf,
                    )
                    dados_agrupados[chave_unica]["municipio"] = municipio
                    dados_agrupados[chave_unica]["sigla_uf"] = sigla_uf

                elif table_id == "mes_categoria_rm":
                    localidade = df[indice_bloco][0]["resultados"][
                        indice_resultado
                    ]["series"][indice_serie]["localidade"]["nome"]
                    regiao_metropolitana = localidade.split(" - ")[0]
                    sigla_uf = localidade.split(" - ")[1]
                    chave_unica = (
                        ano,
                        mes,
                        id_categoria,
                        categoria,
                        regiao_metropolitana,
                        sigla_uf,
                    )
                    dados_agrupados[chave_unica]["regiao_metropolitana"] = (
                        regiao_metropolitana
                    )
                    dados_agrupados[chave_unica]["sigla_uf"] = sigla_uf

                else:
                    chave_unica = (ano, mes, id_categoria, categoria)

                valor_variavel = list(  # noqa: RUF015
                    df[indice_bloco][0]["resultados"][indice_resultado][
                        "series"
                    ][indice_serie]["serie"].values()
                )[0]

                variavel = df[indice_bloco][0]["variavel"]
                if variavel.split(" - ")[1] == "Variação mensal":
                    dados_agrupados[chave_unica]["variacao_mensal"] = (
                        valor_variavel
                    )

                elif variavel.split(" - ")[1] == "Variação acumulada no ano":
                    dados_agrupados[chave_unica]["variacao_anual"] = (
                        valor_variavel
                    )

                elif (
                    variavel.split(" - ")[1]
                    == "Variação acumulada em 12 meses"
                ):
                    dados_agrupados[chave_unica]["variacao_doze_meses"] = (
                        valor_variavel
                    )

                elif variavel.split(" - ")[1] == "Peso mensal":
                    dados_agrupados[chave_unica]["peso_mensal"] = (
                        valor_variavel
                    )

                dados_agrupados[chave_unica]["ano"] = ano
                dados_agrupados[chave_unica]["mes"] = mes
                dados_agrupados[chave_unica]["id_categoria"] = id_categoria
                dados_agrupados[chave_unica]["categoria"] = categoria

                return dados_agrupados


def json_mes_brasil(table_id: str, dataset_id: str, periodo: str):
    input = os.path.join(
        "tmp/json", dataset_id, table_id, periodo, "data.json"
    )
    with open(input) as f:
        df = json.load(f)
    # usamos dicionário auxiliar para juntar na mesma linha
    dados_agrupados = defaultdict(dict)
    print(f"Tratando dataset: {dataset_id} l table_id: {table_id}")
    for indice_bloco in range(len(df)):
        total_resultados = len(df[indice_bloco][0]["resultados"])
        for indice_resultado in range(total_resultados):
            total_series = len(
                df[indice_bloco][0]["resultados"][indice_resultado]["series"]
            )
            for indice_serie in range(total_series):
                chave_serie = list(  # noqa: RUF015
                    df[indice_bloco][0]["resultados"][indice_resultado][
                        "series"
                    ][indice_serie]["serie"].keys()
                )[0]
                ano = chave_serie[0:4]
                mes = chave_serie[4:6]
                # chave única para agrupar
                chave_unica = (ano, mes)

                valor_variavel = list(  # noqa: RUF015
                    df[indice_bloco][0]["resultados"][indice_resultado][
                        "series"
                    ][indice_serie]["serie"].values()
                )[0]

                variavel = df[indice_bloco][0]["variavel"]
                if (
                    variavel.split(" - ")[1]
                    == "Número-índice (base: dezembro de 1993 = 100)"
                ):
                    dados_agrupados[chave_unica]["indice"] = valor_variavel

                elif variavel.split(" - ")[1] == "Variação mensal":
                    dados_agrupados[chave_unica]["variacao_mensal"] = (
                        valor_variavel
                    )

                elif variavel.split(" - ")[1] == "Variação acumulada no ano":
                    dados_agrupados[chave_unica]["variacao_anual"] = (
                        valor_variavel
                    )

                elif (
                    variavel.split(" - ")[1]
                    == "Variação acumulada em 12 meses"
                ):
                    dados_agrupados[chave_unica]["variacao_doze_meses"] = (
                        valor_variavel
                    )

                elif (
                    variavel.split(" - ")[1] == "Variação acumulada em 3 meses"
                ):
                    dados_agrupados[chave_unica]["variacao_trimestral"] = (
                        valor_variavel
                    )

                elif (
                    variavel.split(" - ")[1] == "Variação acumulada em 6 meses"
                ):
                    dados_agrupados[chave_unica]["variacao_semestral"] = (
                        valor_variavel
                    )

                dados_agrupados[chave_unica]["ano"] = ano
                dados_agrupados[chave_unica]["mes"] = mes

    return dados_agrupados


# class to deal with ssl context. See more at:https://github.com/psf/requests/issues/4775
class CustomHttpAdapter(requests.adapters.HTTPAdapter):
    """Transport adapter" that allows us to use custom ssl_context."""

    def __init__(self, ssl_context=None, **kwargs):
        """Initiate the class with the ssl context"""
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        """Initiate the pool manager"""
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context,
        )


def get_legacy_session():
    """Get the session with the ssl context"""
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount("https://", CustomHttpAdapter(ctx))
    return session


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
            "indice argument must be one of the following: 'br_ibge_inpc', 'br_ibge_ipca', 'br_ibge_ipca15'"
        )

    log(f"Checking for updates in {dataset_id} index for {dataset_id}")

    links = {
        "br_ibge_ipca": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n1/all/v/all/p/last%201/c315/7169/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "br_ibge_inpc": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n1/all/v/all/p/last%201/c315/7169/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "br_ibge_ipca15": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n1/all/v/all/p/last%201/c315/7169/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
    }

    links = {key: value for key, value in links.items() if key == dataset_id}

    links_keys = list(links.keys())
    success_dwnl = []

    os.makedirs("/tmp/check_for_updates/", exist_ok=True)

    for key in tqdm(links_keys):
        try:
            response = get_legacy_session().get(links[key])
            # download the csv
            path_key = f"/tmp/check_for_updates/{key}.csv"
            if os.path.exists(path_key):
                pass
            else:
                with open(path_key, "wb") as f:
                    f.write(response.content)
                success_dwnl.append(key)
                log(links[key])
                sleep(5)
        except Exception as e:
            log(e)
            try:
                sleep(5)
                response = get_legacy_session().get(links[key])
                # download the csv
                with open(path_key, "wb") as f:
                    f.write(response.content)
                success_dwnl.append(key)
            except Exception as e:  # pylint: disable=redefined-outer-name
                log(e)
    if len(links_keys) == len(success_dwnl):
        log("All files were successfully downloaded")

    # quebra o flow se houver erro no download de um arquivo.
    else:
        rems = set(links_keys) - set(success_dwnl)
        log(f"The file was not downloaded {rems}")

    file_name = os.listdir("/tmp/check_for_updates")

    file_path = os.path.join("/tmp/check_for_updates/", file_name[0])

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

        data_base = datetime.strptime(str(max_date_bd), "%Y-%m-%d").date()

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


def check_for_update_date(dataset_id, table_id):
    value = check_for_updates(dataset_id=dataset_id, table_id=table_id)

    return value[1]
