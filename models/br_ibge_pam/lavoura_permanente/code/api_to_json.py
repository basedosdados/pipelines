import asyncio
import glob
import json

import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from tqdm import tqdm
from tqdm.asyncio import tqdm  # noqa: F811

API_URL_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]&classificacao={}[{}]"
AGREGADO = "1613"  # É a tabela no SIDRA
PERIODOS = range(1974, 2022 + 1)
VARIAVEIS = [
    "2313",
    "1002313",
    "216",
    "1000216",
    "214",
    "112",
    "215",
    "1000215",
]  # As variáveis da tabela
NIVEL_GEOGRAFICO = "N6"  # N6 = Municipal
LOCALIDADES = "all"
CLASSIFICACAO = "82"  # Código pré-definido por agregado
CATEGORIAS = [
    "2717",
    "2718",
    "45981",
    "2719",
    "2720",
    "2721",
    "40472",
    "2722",
    "2723",
    "31619",
    "31620",
    "40473",
    "2724",
    "2725",
    "2726",
    "2727",
    "2728",
    "2729",
    "2730",
    "2731",
    "2732",
    "2733",
    "2734",
    "2735",
    "2736",
    "2737",
    "2738",
    "2739",
    "2740",
    "90001",
    "2741",
    "2742",
    "2743",
    "2744",
    "2745",
    "2746",
    "2747",
    "2748",
]  # Produtos
ANOS_BAIXADOS = [
    int(glob.os.path.basename(f).split(".")[0])
    for f in glob.glob("../json/*.json")
]
ANOS_RESTANTES = [int(ANO) for ANO in PERIODOS if ANO not in ANOS_BAIXADOS]


async def fetch(session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:  # noqa: F821
    """
    Faz uma requisição GET à API e retorna a resposta em formato JSON.

    Parâmetros:
    - session (aiohttp.ClientSession): A sessão do cliente aiohttp.
    - url (str): A URL da API para a qual a requisição será feita.

    Retorna:
    - Dict[str, Any]: A resposta da API em formato JSON.
    """
    async with session.get(url) as response:
        return await response.json()


async def main(
    years: List[int],  # noqa: F821
    variables: List[str],  # noqa: F821
    categories: List[str],  # noqa: F821
) -> None:
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
        print(f"Consultando dados do ano: {year}")
        async with aiohttp.ClientSession(
            connector=TCPConnector(limit=100, force_close=True),
            timeout=ClientTimeout(total=1200),
        ) as session:
            tasks = []
            for variable in variables:
                for category in categories:
                    url = API_URL_BASE.format(
                        AGREGADO,
                        year,
                        variable,
                        NIVEL_GEOGRAFICO,
                        LOCALIDADES,
                        CLASSIFICACAO,
                        category,
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
    asyncio.run(main(ANOS_RESTANTES, VARIAVEIS, CATEGORIAS))
