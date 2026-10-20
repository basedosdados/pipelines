"""
Tasks for br_bcb_taxa_selic.
"""

import os

import pandas as pd
import requests
from prefect import task

from pipelines.datasets.br_bcb_taxa_selic.constants import COVERAGE, TABLE_ID
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json"

# BCB rejeita requests sem User-Agent (HTTP 406).
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


@task(retries=3, retry_delay_seconds=10)
def download_selic_data() -> pd.DataFrame:
    """Baixa a série completa da Selic (BCB SGS 11) e já trata a coluna de data.

    A API não aceita filtro por intervalo nessa série — toda chamada
    devolve a série inteira. `get_latest_update` e `extract_load_data`
    cada um chama isso de forma independente (custo de download
    duplicado aceito, variante `check_and_download` foi cancelada).

    Returns:
        DataFrame com a coluna `data` já convertida pra `date`.
    """
    response = requests.get(API_URL, headers=_HEADERS, timeout=30)
    response.raise_for_status()
    df = pd.DataFrame(response.json())
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.date
    return df


def get_latest_update() -> SourceInspection:
    """Baixa a série da Selic só pra descobrir a data de referência mais recente.

    Returns:
        `SourceInspection` com a data máxima encontrada na coluna `data`.
    """
    df = download_selic_data()
    return SourceInspection(reference_date=df["data"].max())


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Baixa a série da Selic de novo e escreve o CSV pra upload.

    Args:
        download_params: recebido do check_update — não usado aqui, a
            série não aceita filtro por data (sempre vem inteira).

    Returns:
        `ExtractAndLoad` com a coverage da tabela e o caminho do CSV.
    """
    df = download_selic_data()

    folder = f"tmp/{TABLE_ID}/output"
    os.makedirs(folder, exist_ok=True)
    filepath = f"{folder}/{TABLE_ID}.csv"
    df.to_csv(filepath, index=False)
    print(f"Dados tratados: {len(df)} linhas → {filepath}")

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=filepath,
    )
