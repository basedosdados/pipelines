import asyncio
import json
import os
import ssl
from collections import defaultdict
from typing import Any

import aiohttp
import numpy as np
import requests
import urllib3
from aiohttp import ClientTimeout, TCPConnector
from tqdm.asyncio import tqdm as tqdm_asyncio

from pipelines.crawler.ibge_inflacao.constants import constants


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
    else:
        os.makedirs(output_path, exist_ok=True)

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
            print(
                f"Consultando período {period} | tabela: {table_id} | dataset_id: {dataset_id}"
            )
            for aggregate in aggregates:
                for variable in variables:
                    print(
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

    # dicionário auxiliar para juntar na mesma linha
    dados_agrupados = defaultdict(dict)

    for indice_bloco in range(len(df)):
        total_resultados = len(df[indice_bloco][0]["resultados"])
        for indice_resultado in range(total_resultados):
            total_series = len(
                df[indice_bloco][0]["resultados"][indice_resultado]["series"]
            )
            for indice_serie in range(total_series):
                chave_serie = list(
                    df[indice_bloco][0]["resultados"][indice_resultado][
                        "series"
                    ][indice_serie]["serie"].keys()
                )[0]
                ano = chave_serie[0:4]
                mes = chave_serie[4:6]

                categoria_valor = list(
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

                valor_variavel = list(
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
                chave_serie = list(
                    df[indice_bloco][0]["resultados"][indice_resultado][
                        "series"
                    ][indice_serie]["serie"].keys()
                )[0]
                ano = chave_serie[0:4]
                mes = chave_serie[4:6]
                # chave única para agrupar
                chave_unica = (ano, mes)

                valor_variavel = list(
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
