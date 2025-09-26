import asyncio
import json
import os
from typing import Any, Dict, List

import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from tqdm.asyncio import tqdm as tqdm_asyncio


def build_url(
    aggregate: int, period: str, variable: int, geo_level: str
) -> str:
    """Monta a URL de consulta da API IBGE."""
    return (
        f"https://servicodados.ibge.gov.br/api/v3/agregados/{aggregate}"
        f"/periodos/{period}/variaveis/{variable}"
        f"?localidades={geo_level}&classificacao=315[all]"
    )


async def fetch(session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
    """Executa requisição GET e retorna a resposta em JSON."""
    async with session.get(url) as response:
        return await response.json()


def save_json(
    data: List[Dict[str, Any]],
    dataset_id: str,
    table_id: str,
    period: str,
    output_dir: str,
) -> None:
    """Salva os dados em arquivo JSON no diretório definido."""
    output_path = os.path.join(output_dir, dataset_id, table_id, period)
    os.makedirs(output_path, exist_ok=True)

    file_path = f"{output_path}/data.json"
    with open(file_path, "a") as f:
        json.dump(data, f)


async def collect_data(
    dataset_id: str,
    table_id: str,
    aggregates: List[int],
    variables: List[int],
    periods: List[str],
    geo_level: str,
) -> None:
    """Consulta dados da API IBGE e salva os resultados em JSON."""
    async with aiohttp.ClientSession(
        connector=TCPConnector(limit=100, force_close=True),
        timeout=ClientTimeout(total=1200),
    ) as session:
        for period in periods:
            print(
                f"Consultando período {period} | tabela: {table_id} | dataset_id: {dataset_id}"
            )

            tasks = [
                fetch(
                    session, build_url(aggregate, period, variable, geo_level)
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
                period=period,  # ← só o período atual
                output_dir=os.path.join(os.getcwd(), "tmp/json"),
            )  # ← troquei nome do parâmetro
