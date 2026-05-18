import pandas as pd
import requests
from bs4 import BeautifulSoup


def get_html_table(url, n_rows):
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("div", class_="table")

    if not table:
        print("Tabela não encontrada.")
    else:
        headers = [
            cell.get_text(strip=True)
            for cell in table.find("div", class_="row header").find_all(
                "div", class_="cell"
            )
        ]

        if n_rows is not None:
            rows = table.find_all("div", class_="row")[1 : n_rows + 1]
        else:
            rows = table.find_all("div", class_="row")[1:]

        data = [
            [
                cell.get_text(strip=True)
                for cell in row.find_all("div", class_="cell")
            ]
            for row in rows
        ]
        df = pd.DataFrame(data, columns=headers)
        return df


def request_data(url, filename):
    url = f"{url}{filename}"
    data = pd.read_csv(url)
    return data


def extract_all_data(url, n_rows=None):
    full_data = pd.DataFrame()
    table = get_html_table(url, n_rows)
    for row in table["Nome"]:
        file_data = request_data(url, row)
        full_data = pd.concat([full_data, file_data], axis=0)
    return full_data


if __name__ == "__main__":
    import os

    if not os.path.exists("./input"):
        os.makedirs("./input")

    # Month Data
    month_url = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/"

    print("Buscando arquivos no site do INPE...")
    table = get_html_table(month_url, None)

    # Pega os arquivos de 2025 e 2026
    files_to_download = [
        row for row in table["Nome"] if "2025" in row or "2026" in row
    ]

    month_data = pd.DataFrame()
    for file in files_to_download:
        print(f"Baixando: {file}")
        file_data = request_data(month_url, file)
        month_data = pd.concat([month_data, file_data], axis=0)

    month_data.to_csv("./input/month_fire_data_new.csv", index=False)
    print("Download concluído com sucesso!")

    # Year Data - Comentado para não baixar dados antigos de 2003 a 2025
    # year_url = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/Brasil_sat_ref/"
    # year_data = extract_all_data(year_url, 20)
    # year_data.to_csv("./input/year_fire_data.csv")
