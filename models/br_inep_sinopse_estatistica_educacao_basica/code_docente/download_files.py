# -*- coding: utf-8 -*-
import os
import zipfile
import pandas as pd
import basedosdados as bd
import requests

INPUT = os.path.join(os.getcwd(), "input")
OUTPUT = os.path.join(os.getcwd(), "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

for year in range(2019, 2023):

#     URL = f"https://download.inep.gov.br/dados_abertos/sinopses_estatisticas/sinopses_estatisticas_censo_escolar_{year}.zip"
    URL = f"https://download.inep.gov.br/informacoes_estatisticas/sinopses_estatisticas/sinopses_educacao_basica/sinopse_estatistica_da_educacao_basica_{year}.zip"

    print(URL)
    rq = requests.get(URL)
    with open(os.path.join(INPUT, os.path.basename(URL)), "wb") as f:
        f.write(rq.content)
        print(os.path.join(INPUT, os.path.basename(URL)))
    with zipfile.ZipFile(os.path.join(INPUT, os.path.basename(URL)), 'r') as z:
        z.extractall(OUTPUT)
