# -*- coding: utf-8 -*-
import pandas as pd
import requests
from bs4 import BeautifulSoup


def getHtmlTable(url, nRows):
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

        if nRows is not None:
            rows = table.find_all("div", class_="row")[1 : nRows + 1]
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


def requestData(url, fileName):
    url = f"{url}{fileName}"
    data = pd.read_csv(url)
    return data


def extractAllData(url, nRows=None):
    fullData = pd.DataFrame()
    table = getHtmlTable(url, nRows)
    for row in table["Nome"]:
        fileData = requestData(url, row)
        fullData = pd.concat([fullData, fileData], axis=0)
    return fullData


if __name__ == "__main__":
    # Month Data
    month_url = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/"
    month_data = extractAllData(month_url)
    month_data.to_csv("./input/month_fire_data.csv")

    # Year Data
    year_url = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/Brasil_sat_ref/"
    year_data = extractAllData(year_url, 20)
    year_data.to_csv("./input/year_fire_data.csv")
