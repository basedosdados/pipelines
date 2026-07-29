"""
Tasks para o flow de teste end-to-end (test_dataset).
Fonte: API do BCB — taxa de câmbio USD/BRL (série 1).
"""

from pathlib import Path

import pandas as pd
import requests
from prefect import task


@task
def download_taxa_cambio(n_days: int = 30) -> Path:
    """Baixa os últimos n_days dias de taxa de câmbio USD/BRL da API do BCB."""
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados/ultimos/{n_days}?formato=json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data)
    df.columns = ["data", "valor"]
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.strftime(
        "%Y-%m-%d"
    )
    df["valor"] = df["valor"].str.replace(",", ".").astype(float)

    output_dir = Path("/tmp/test_dataset/taxa_cambio")
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "taxa_cambio.csv"
    df.to_csv(filepath, index=False)

    print(f"Baixados {len(df)} registros. Último: {df.iloc[-1].to_dict()}")
    return filepath
