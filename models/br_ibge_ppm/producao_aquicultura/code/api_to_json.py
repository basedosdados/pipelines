# -*- coding: utf-8 -*-
import asyncio
import aiohttp
import json
import glob
from tqdm.asyncio import tqdm
from aiohttp import ClientTimeout, TCPConnector
from typing import Any, Dict, List

API_URL_BASE        = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]&classificacao={}[{}]"
AGREGADO            = "3940" # É a tabela no SIDRA
PERIODOS            = range(2013, 2022 + 1)
VARIAVEIS           = ["4146", "215"] # As variáveis da tabela
NIVEL_GEOGRAFICO    = "N6" # N6 = Municipal
LOCALIDADES         = "all"
CLASSIFICACAO       = "654" # Código pré-definido por agregado
CATEGORIAS          = ["32861", "32865", "32866", "32867", "32868", "32869", "32870",
                       "32871", "32872", "32873", "32874", "32875", "32876", "32877",
                       "32878", "32879", "32880", "32881", "32886", "32887", "32888",
                       "32889", "32890", "32891"] # Produtos
ANOS_BAIXADOS       = [int(glob.os.path.basename(f).split(".")[0]) for f in glob.glob(f"../json/*.json")]
ANOS_RESTANTES      = [int(ANO) for ANO in PERIODOS if ANO not in ANOS_BAIXADOS]

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
        return {'url': url, 'data': data}

async def main(years: List[int], variables: List[str], categories: List[str]) -> None:
    """
    Faz requisições para a API para cada ano, variável e categoria, salvando as respostas em arquivos JSON.

    Parâmetros:
    - years (List[int]): Lista de anos para os quais os dados serão consultados.
    - variables (List[str]): Lista de variáveis da tabela a serem consultadas.
    - categories (List[str]): Lista de categorias a serem consultadas.

    Retorna:
    - None
    """
    for year in years:
        print(f'Consultando dados do ano: {year}')
        async with aiohttp.ClientSession(connector=TCPConnector(limit=100, force_close=True), timeout=ClientTimeout(total=1200)) as session:
            tasks = []
            for variable in variables:
                for category in categories:
                    url = API_URL_BASE.format(AGREGADO, year, variable, NIVEL_GEOGRAFICO, LOCALIDADES, CLASSIFICACAO, category)
                    task = fetch(session, url)
                    tasks.append(asyncio.ensure_future(task))
            responses = []
            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                try:
                    response = await future
                    responses.append(response)
                except asyncio.TimeoutError:
                    print(f"Request timed out for {url}")
            with open(f'../json/{year}.json', 'a') as f:
                json.dump(responses, f)

if __name__ == "__main__":
    asyncio.run(main(ANOS_RESTANTES, VARIAVEIS, CATEGORIAS))
