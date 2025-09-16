# -*- coding: utf-8 -*-
import asyncio
import json
import os
from pathlib import Path

import aiohttp
import pandas as pd
from aiohttp import ClientTimeout, TCPConnector
from tqdm import tqdm
from tqdm.asyncio import tqdm  # noqa: F811

# Mudando o diretório de trabalho para o diretório do script
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Criando a pasta de output
path_json_output = "output/silvicultura/json"
os.makedirs(path_json_output, exist_ok=True)

API_URL_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados/291/periodos/{}/variaveis/{}?localidades=N6[all]&classificacao=194[{}]"
PERIODOS = range(2023, 2023 + 1)  # Ano de 2023
VARIAVEIS = "142|143"  # 142: Quantidade produzida na silvicultura + 143: Valor da produção na silvicultura
CATEGORIAS = pd.read_csv("silvicultura_metadados_enriquecidos.csv", dtype=str)[
    "id"
].tolist()
BAIXADOS = [
    int(x.stem) for x in Path("./output/silvicultura/json/").glob("*.json")
]


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()


async def main(years, categories):
    for year in years:
        print(f"Consultando dados do ano: {year}")
        async with aiohttp.ClientSession(
            connector=TCPConnector(limit=50, force_close=True),
            timeout=ClientTimeout(total=600),
        ) as session:
            tasks = []
            for category in categories:
                url = API_URL_BASE.format(year, VARIAVEIS, category)
                task = fetch(session, url)
                tasks.append(asyncio.ensure_future(task))
            responses = []
            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                try:
                    response = await future
                    responses.append(response)
                except asyncio.TimeoutError:
                    print(f"Request timed out for {url}")
            with open(f"./output/silvicultura/json/{year}.json", "a") as f:
                json.dump(responses, f)


if __name__ == "__main__":
    years = [ano for ano in PERIODOS if ano not in BAIXADOS]
    categories = CATEGORIAS
    asyncio.run(main(years, categories))
