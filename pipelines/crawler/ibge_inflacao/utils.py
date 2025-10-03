import asyncio
import json
import os
from collections import defaultdict
from datetime import date
from datetime import datetime as dt
from typing import Any

import aiohttp
import numpy as np
import pandas as pd
from aiohttp import ClientTimeout, TCPConnector
from dateutil.relativedelta import relativedelta
from tqdm.asyncio import tqdm as tqdm_asyncio

from pipelines.crawler.ibge_inflacao.constants import constants
from pipelines.utils.metadata.tasks import task_get_api_most_recent_date
from pipelines.utils.utils import log


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
    output_dir: str,
) -> None:
    """Salva os dados em arquivo JSON no diretório definido."""
    output_path = os.path.join(output_dir, dataset_id, table_id)
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
                output_dir=os.path.join(os.getcwd(), constants.INPUT.value),
            )


def json_categoria(table_id: str, dataset_id: str) -> defaultdict:
    input = os.path.join(
        constants.INPUT.value, dataset_id, table_id, "data.json"
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


def json_mes_brasil(table_id: str, dataset_id: str) -> defaultdict:
    input = os.path.join(
        constants.INPUT.value, dataset_id, table_id, "data.json"
    )
    with open(input) as f:
        df = json.load(f)
    # usamos dicionário auxiliar para juntar na mesma linha
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


def order_by_columns(df: pd.DataFrame, table_id: str) -> list:
    if table_id == "mes_brasil":
        rename = [
            "ano",
            "mes",
            "indice",
            "variacao_mensal",
            "variacao_trimestral",
            "variacao_semestral",
            "variacao_anual",
            "variacao_doze_meses",
        ]
    if table_id == "mes_categoria_brasil":
        rename = [
            "ano",
            "mes",
            "categoria",
            "categoria",
            "peso_mensal",
            "variacao_mensal",
            "variacao_anual",
            "variacao_doze_meses",
        ]
    if table_id == "mes_categoria_municipio":
        rename = [
            "ano",
            "mes",
            "id_municipio",
            "sigla_uf",
            "id_categoria",
            "categoria",
            "peso_mensal",
            "variacao_mensal",
            "variacao_anual",
            "variacao_doze_meses",
        ]

    if table_id == "mes_categoria_rm":
        rename = [
            "ano",
            "mes",
            "regiao_metropolitana",
            "sigla_uf",
            "id_categoria",
            "categoria",
            "peso_mensal",
            "variacao_mensal",
            "variacao_anual",
            "variacao_doze_meses",
        ]

    return rename


def get_date_api(dataset_id: str, table_id: str) -> tuple[date, str]:
    input = os.path.join(
        constants.INPUT.value, dataset_id, table_id, "data.json"
    )
    with open(input) as f:
        df = json.load(f)

    chave_serie = next(
        iter(df[0][0]["resultados"][0]["series"][0]["serie"].keys())
    )
    ano = chave_serie[0:4]
    mes = chave_serie[4:6]

    date_original = f"{ano}-{mes}-01"

    return dt.strptime(str(date_original), "%Y-%m-%d").date()


def next_date_update(
    dataset_id: str,
    table_id: str,
) -> str:
    dt = task_get_api_most_recent_date.run(
        dataset_id=dataset_id, table_id=table_id, date_format="%Y-%m"
    )

    data_proxima = dt + relativedelta(months=+1)
    ano = str(data_proxima.year)
    mes = f"{data_proxima.month:02d}"

    return ano + mes
