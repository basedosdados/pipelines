# -*- coding: utf-8 -*-
import asyncio
import json
from pathlib import Path

import aiohttp
import pandas as pd
from aiohttp import ClientTimeout, TCPConnector
from tqdm import tqdm
from tqdm.asyncio import tqdm

API_URL_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados/289/periodos/{}/variaveis/{}?localidades=N6[all]&classificacao=193[{}]"
PERIODOS = range(1986, 2022 + 1)  # De 1986 a 2022
VARIAVEIS = "144|145"  # 144: Quantidade produzida na extração vegetal + 145: Valor da produção na extração vegetal
CATEGORIAS = pd.read_csv(
    "extracao_vegetal_metadados_enriquecidos.csv", dtype=str
)["id"].tolist()
BAIXADOS = [
    int(x.stem) for x in Path("./output/extracao_vegetal/json/").glob("*.json")
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
            with open(f"./output/extracao_vegetal/json/{year}.json", "a") as f:
                json.dump(responses, f)


if __name__ == "__main__":
    years = [ano for ano in PERIODOS if ano not in BAIXADOS]
    categories = CATEGORIAS
    asyncio.run(main(years, categories))
