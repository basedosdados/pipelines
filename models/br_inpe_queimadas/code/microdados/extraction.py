import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

# URL base dos dados mensais no servidor do INPE
MONTH_URL = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/"


# Faz scraping da tabela HTML do servidor INPE e retorna os arquivos disponíveis como DataFrame
def get_html_table(url, n_rows=None):
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("div", class_="table")

    if not table:
        raise ValueError(f"Tabela não encontrada em: {url}")

    headers = [
        cell.get_text(strip=True)
        for cell in table.find("div", class_="row header").find_all(
            "div", class_="cell"
        )
    ]

    rows = table.find_all("div", class_="row")[
        1 : n_rows + 1 if n_rows else None
    ]

    data = [
        [
            cell.get_text(strip=True)
            for cell in row.find_all("div", class_="cell")
        ]
        for row in rows
    ]
    return pd.DataFrame(data, columns=headers)


# Baixa um único arquivo CSV do servidor INPE
def request_data(url, filename):
    return pd.read_csv(f"{url}{filename}")


# Baixa os arquivos mensais do INPE filtrando pelos anos solicitados e salva em input_dir
def download(year_range: list[int], input_dir: str) -> None:
    os.makedirs(input_dir, exist_ok=True)

    # Lista todos os arquivos disponíveis no servidor
    table = get_html_table(MONTH_URL)

    # Filtra apenas os arquivos que contêm o ano desejado no nome
    files_to_download = [
        row
        for row in table["Nome"]
        if any(str(year) in row for year in year_range)
    ]

    # Baixa cada arquivo individualmente e concatena
    month_data = pd.DataFrame()
    for file in files_to_download:
        print(f"  Baixando: {file}")
        file_data = request_data(MONTH_URL, file)
        month_data = pd.concat([month_data, file_data], axis=0)

    output_path = f"{input_dir}/month_fire_data_new.csv"
    month_data.to_csv(output_path, index=False)
    print(f"  Download concluído → {output_path}")
