"""
Utilitário compartilhado pelos índices de inflação do IBGE
(`br_ibge_ipca`, `br_ibge_ipca15`, `br_ibge_inpc`).

Os 3 datasets chamam exatamente as mesmas 3 tasks (`check_for_updates`,
`collect_data_utils`, `json_to_csv`), parametrizadas só por
`dataset_id`/`table_id` — a diferença entre eles é só lookup nas constantes
(`DATASETS`, `DATASETS_MES_BRASIL`, `GEO_LEVELS`), sem nenhuma lógica
condicional por dataset. Antes vivia num diretório próprio dentro de
`pipelines/crawler/` (`constants.py` + `utils.py` + `tasks.py`), migrado pra
cá como parte da eliminação progressiva de `pipelines/crawler/`.

## Em relação ao parâmetro `force_run`

Nos flows dos datasets, mantenha `force_run=false` (default). O
`force_run=true` ignora o guard de "há dado novo?" e força a chamada à API
mesmo quando não há período novo pra baixar. Para a API de agregados do IBGE
(SIDRA, `servicodados.ibge.gov.br/api/v3/agregados`), isso é problemático por
dois motivos:

1. **Janela inexistente.** Quando não há mês novo publicado, a janela
   calculada por `json_mes_brasil`/`next_date_update` pode pedir um período
   que ainda não existe na API. O retorno vem sem o bloco `resultados`
   esperado, e o crawler estoura com `IndexError: list index out of range`.
2. **Limite de 100.000 valores por requisição** (ver seção abaixo). Forçar
   runs fora do schedule aumenta o risco de bater esse limite e receber
   `HTTP 500`.

Em produção, sempre deixe o guard decidir se há trabalho. Use
`force_run=true` apenas para debug local com janela controlada.

## A API de agregados do IBGE impõe restrição de tamanho à requisição

Citação direta da
[documentação oficial](https://servicodados.ibge.gov.br/api/docs/agregados?versao=3):

> Para *Variáveis por agregado, períodos pesquisados e identificador da
> variável* e *Variáveis por agregado e identificador da variável*, cada
> requisição permite retornar no máximo **100.000 valores**. Para saber a
> quantidade de valores retornados, use a seguinte fórmula:
>
> `Nº de categorias x Nº de períodos x Nº de localidades <= 100.000`
>
> Se a requisição exceder 100.000 valores, a API retorna **HTTP 500**.

Implicação prática: a tabela `mes_categoria_municipio` chega perto do
limite (`categorias x meses x municípios` cresce rápido). `collect_data`
chunka as requisições por período pra se manter abaixo do teto. Ao alterar
o cálculo da janela ou o conjunto de localidades, recalcule a estimativa
antes de subir. Em caso de `HTTP 500` em produção, a primeira suspeita é
estouro do teto — não retry cego.
"""

import asyncio
import json
import os
from collections import defaultdict
from datetime import date
from datetime import datetime as dt
from enum import Enum
from typing import Any

import aiohttp
import numpy as np
import pandas as pd
from aiohttp import ClientTimeout, TCPConnector
from dateutil.relativedelta import relativedelta
from prefect import task
from tqdm.asyncio import tqdm as tqdm_asyncio

from pipelines.utils.metadata.tasks import task_get_api_most_recent_date
from pipelines.utils.utils import log, to_partitions


class constants(Enum):
    DATASETS = {
        "br_ibge_ipca15": {
            "aggregates": [7062],
            "variables": [355, 356, 1120, 357],
        },
        "br_ibge_ipca": {
            "aggregates": [7060],
            "variables": [63, 69, 2265, 66],
        },
        "br_ibge_inpc": {
            "aggregates": [7063],
            "variables": [44, 68, 2292, 45],
        },
    }

    DATASETS_MES_BRASIL = {
        "br_ibge_ipca15": {
            "aggregates": [
                3065
            ],  # IPCA15 - Série histórica com número-índice, variação mensal e variações acumuladas em 3 meses, em 6 meses, no ano e em 12 meses (a partir de maio/2000)
            "variables": [
                1117,  # - IPCA15 - Número-índice (base: dezembro de 1993 = 100)
                355,  # - IPCA15 - Variação mensal
                1118,  # - IPCA15 - Variação acumulada em 3 meses
                1119,  # - IPCA15 - Variação acumulada em 6 meses
                356,  # - IPCA15 - Variação acumulada no ano
                1120,  # - IPCA15 - Variação acumulada em 12 meses
            ],
        },
        "br_ibge_ipca": {
            "aggregates": [
                1737
            ],  # IPCA - Série histórica com número-índice, variação mensal e variações acumuladas em 3 meses, em 6 meses, no ano e em 12 meses (a partir de dezembro/1979)
            "variables": [
                2266,  # - IPCA - Número-índice (base: dezembro de 1993 = 100)
                63,  # - IPCA - Variação mensal
                2263,  # - IPCA - Variação acumulada em 3 meses
                2264,  # - IPCA - Variação acumulada em 6 meses
                69,  # - IPCA - Variação acumulada no ano
                2265,  # - IPCA - Variação acumulada em 12 meses
            ],
        },
        "br_ibge_inpc": {
            "aggregates": [
                1736
            ],  # INPC - Série histórica com número-índice, variação mensal e variações acumuladas em 3 meses, em 6 meses, no ano e em 12 meses (a partir de abril/1979)
            "variables": [
                2289,  # - INPC - Número-índice (base: dezembro de 1993 = 100)
                44,  # - INPC - Variação mensal
                2290,  # - INPC - Variação acumulada em 3 meses
                2291,  # - INPC - Variação acumulada em 6 meses
                68,  # - INPC - Variação acumulada no ano
                2292,  # - INPC - Variação acumulada em 12 meses
            ],
        },
    }

    GEO_LEVELS_MES_BRASIL = "N1[all]"

    GEO_LEVELS = {
        "mes_categoria_brasil": "N1[all]",  # Brasil
        "mes_categoria_municipio": "N6[all]",  # Município
        "mes_categoria_rm": "N7[all]",  # Região Metropolitana
    }

    OUTPUT = "tmp/output"
    INPUT = "tmp/json"


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
                            aggregate,
                            # pyrefly: ignore [bad-argument-type]
                            period,
                            variable,
                            geo_level,
                            table_id,
                        )
                    )

            tasks = [
                fetch(
                    session,
                    build_url(
                        aggregate,
                        # pyrefly: ignore [bad-argument-type]
                        period,
                        variable,
                        geo_level,
                        table_id,
                    ),
                )
                for aggregate in aggregates
                for variable in variables
            ]

            results = []
            for result in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
                try:
                    results.append(await result)
                except TimeoutError:
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
        if not df[indice_bloco]:
            continue

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
                    # pyrefly: ignore [bad-index]
                    dados_agrupados[chave_unica]["variacao_mensal"] = (
                        valor_variavel
                    )

                elif variavel.split(" - ")[1] == "Variação acumulada no ano":
                    # pyrefly: ignore [bad-index]
                    dados_agrupados[chave_unica]["variacao_anual"] = (
                        valor_variavel
                    )

                elif (
                    variavel.split(" - ")[1]
                    == "Variação acumulada em 12 meses"
                ):
                    # pyrefly: ignore [bad-index]
                    dados_agrupados[chave_unica]["variacao_doze_meses"] = (
                        valor_variavel
                    )

                elif variavel.split(" - ")[1] == "Peso mensal":
                    # pyrefly: ignore [bad-index]
                    dados_agrupados[chave_unica]["peso_mensal"] = (
                        valor_variavel
                    )

                # pyrefly: ignore [bad-index]
                dados_agrupados[chave_unica]["ano"] = ano
                # pyrefly: ignore [bad-index]
                dados_agrupados[chave_unica]["mes"] = mes
                # pyrefly: ignore [bad-index]
                dados_agrupados[chave_unica]["id_categoria"] = id_categoria
                # pyrefly: ignore [bad-index]
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
        if not df[indice_bloco]:
            continue

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


def order_by_columns(table_id: str) -> list:
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
            "id_categoria",
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
            "municipio",
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

    # pyrefly: ignore [unbound-name]
    return rename


def get_date_api(dataset_id: str, table_id: str) -> date:
    input = os.path.join(
        constants.INPUT.value, dataset_id, table_id, "data.json"
    )
    with open(input) as f:
        df = json.load(f)

    try:
        if not df or not df[0]:
            raise ValueError(
                f"[ibge_inflacao] Bloco vazio em get_date_api para {dataset_id}.{table_id} — mês provavelmente não publicado."
            )

        chave_serie = next(
            iter(df[0][0]["resultados"][0]["series"][0]["serie"].keys())
        )
        ano = chave_serie[0:4]
        mes = chave_serie[4:6]

        date_original = f"{ano}-{mes}-01"

        return dt.strptime(date_original, "%Y-%m-%d").date()

    except Exception as e:
        log(f"Não há dados recentes na API: {e}")
        return task_get_api_most_recent_date.fn(
            dataset_id=dataset_id, table_id=table_id, date_format="%Y-%m"
        )


def next_date_update(
    dataset_id: str,
    table_id: str,
) -> str:
    dt = task_get_api_most_recent_date.fn(
        dataset_id=dataset_id, table_id=table_id, date_format="%Y-%m"
    )

    data_proxima = dt + relativedelta(months=+1)
    ano = str(data_proxima.year)
    mes = f"{data_proxima.month:02d}"

    log(f"Próximo período a ser atualizado: {ano}-{mes}")

    return ano + mes


@task
def check_for_updates(
    table_id: str,
    dataset_id: str,
) -> date:
    return get_date_api(table_id=table_id, dataset_id=dataset_id)


@task
def collect_data_utils(
    dataset_id: str, table_id: str, periodo: str | None = None
) -> str:
    if periodo is None:
        periodo = next_date_update(
            dataset_id=dataset_id,
            table_id=table_id,
        )

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
                # pyrefly: ignore [bad-argument-type]
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
                # pyrefly: ignore [bad-argument-type]
                periods=periodo,
                geo_level=constants.GEO_LEVELS.value[table_id],
            )
        )

    return periodo


@task
def json_to_csv(table_id: str, dataset_id: str) -> str:
    if table_id == "mes_brasil":
        dados_agrupados = json_mes_brasil(
            dataset_id=dataset_id, table_id=table_id
        )
    else:
        dados_agrupados = json_categoria(
            dataset_id=dataset_id, table_id=table_id
        )

    df = pd.DataFrame(list(dados_agrupados.values()))
    df = df.apply(lambda x: x.replace("-", np.nan))

    df = df[order_by_columns(table_id=table_id)]

    output_path = os.path.join(constants.OUTPUT.value, dataset_id, table_id)
    to_partitions(
        data=df,
        savepath=output_path,
        partition_columns=["ano", "mes"],
    )

    return output_path
