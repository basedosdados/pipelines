# -*- coding: utf-8 -*-
import asyncio
import glob
import json
from typing import Any, Dict, List

import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from tqdm import tqdm
from tqdm.asyncio import tqdm  # noqa: F811

API_URL_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]"
AGREGADO = "95"  # É a tabela no SIDRA
PERIODOS = range(1974, 2022 + 1)
VARIAVEIS = ["108"]  # As variáveis da tabela
NIVEL_GEOGRAFICO = "N6"  # N6 = Municipal
LOCALIDADES = "all"
ANOS_BAIXADOS = [
    int(glob.os.path.basename(f).split(".")[0])
    for f in glob.glob("../json/*.json")
]
ANOS_RESTANTES = [int(ANO) for ANO in PERIODOS if ANO not in ANOS_BAIXADOS]


async def fetch(session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
    """
    Faz uma requisição GET à API e retorna a resposta em formato JSON, incluindo a URL usada.

    Parâmetros:
    - session (aiohttp.ClientSession): A sessão do cliente aiohttp.
    - url (str): A URL da API para a qual a requisição será feita.

    Retorna:
    - Dict[str, Any]: A resposta da API em formato JSON, incluindo a URL usada.
    """
    async with session.get(url) as response:
        data = await response.json()
        return {"url": url, "data": data}


async def main(years: List[int], variables: List[str]) -> None:
    """
    Faz requisições para a API para cada ano e variável, salvando as respostas em arquivos JSON.

    Parâmetros:
    - years (List[int]): Lista de anos para os quais os dados serão consultados.
    - variables (List[str]): Lista de variáveis da tabela a serem consultadas.

    Retorna:
    - None
    """
    for year in years:
        print(f"Consultando dados do ano: {year}")
        async with aiohttp.ClientSession(
            connector=TCPConnector(limit=100, force_close=True),
            timeout=ClientTimeout(total=1200),
        ) as session:
            tasks = []
            for variable in variables:
                url = API_URL_BASE.format(
                    AGREGADO, year, variable, NIVEL_GEOGRAFICO, LOCALIDADES
                )
                task = fetch(session, url)
                tasks.append(asyncio.ensure_future(task))
            responses = []
            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                try:
                    response = await future
                    responses.append(response)
                except asyncio.TimeoutError:
                    print(f"Request timed out for {url}")
            with open(f"../json/{year}.json", "a") as f:
                json.dump(responses, f)


if __name__ == "__main__":
    asyncio.run(main(ANOS_RESTANTES, VARIAVEIS))
