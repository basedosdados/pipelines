"""
Esse script atualiza as seguintes tabelas do dataset br_inep_educacao_especial
para 2024.

- tempo_ensino
- localizacao
- etapa_ensino_serie
- faixa_etaria
- sexo_raca_cor
"""

import os
import zipfile
from pathlib import Path

import pandas as pd
import requests

ROOT = Path("models") / "br_inep_educacao_especial"
INPUT = ROOT / "input"
OUTPUT = ROOT / "output"

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

URL = "https://download.inep.gov.br/dados_abertos/sinopses_estatisticas/sinopses_estatisticas_censo_escolar_2024.zip"

r = requests.get(
    URL, headers={"User-Agent": "Mozilla/5.0"}, verify=False, stream=True
)

with open(INPUT / "2024.zip", "wb") as fd:
    for chunk in r.iter_content(chunk_size=128):
        fd.write(chunk)

with zipfile.ZipFile(INPUT / "2024.zip") as z:
    z.extractall(INPUT)


def read_sheet(sheet_name: str, skiprows: int = 8) -> pd.DataFrame:
    return pd.read_excel(
        INPUT
        / "sinopse_estatistica_censo_escolar_2024"
        / "Sinopse_Estatistica_da_Educação_Basica_2024.xlsx",
        skiprows=skiprows,
        sheet_name=sheet_name,
    )
