# -*- coding: utf-8 -*-
import concurrent.futures
import os
from itertools import product

import requests


class DownloaderBrInpeProdes:
    def __init__(self, year: int):
        self.year = year
        url_base = f"http://www.dpi.inpe.br/prodesdigital/tabelatxt.php?ano={self.year}"
        self.biomas_urls = {
            "Amazônia": f"{url_base}&estado=&bioma=Amaz%C3%B4nia&ordem=municipio&type=tabela&output=txt&",
            "Caatinga": f"{url_base}&estado=&bioma=Caatinga&ordem=municipio&type=tabela&output=txt&",
            "Cerrado": f"{url_base}&estado=&bioma=Cerrado&ordem=municipio&type=tabela&output=txt&",
            "Mata Atlântica": f"{url_base}&estado=&bioma=Mata%20Atl%C3%A2ntica&ordem=municipio&type=tabela&output=txt&",
            "Pampa": f"{url_base}&estado=&bioma=Pampa&ordem=municipio&type=tabela&output=txt&",
            "Pantanal": f"{url_base}&estado=&bioma=Pantanal&ordem=municipio&type=tabela&output=txt&",
        }

    def url(self, bioma: str) -> str:
        return self.biomas_urls[bioma]

    def download_data(self, bioma: str, format="csv") -> None:
        url = self.url(bioma)
        response = requests.get(url)
        response.raise_for_status()
        os.makedirs("../input", exist_ok=True)
        with open(f"../input/{bioma}_{self.year}.{format}", "w") as f:
            content_as_string = response.content.decode()
            f.write(content_as_string)

        print(f"Successfully downloaded for bioma {bioma} year {self.year}")


def start_downloader(info: tuple) -> None:
    DownloaderBrInpeProdes(info[0]).download_data(info[1])


def download_all_biomas_data() -> None:
    years = range(2000, 2025)
    biomas_names = [
        "Amazônia",
        "Caatinga",
        "Cerrado",
        "Mata Atlântica",
        "Pampa",
        "Pantanal",
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(start_downloader, product(years, biomas_names))


if __name__ == "__main__":
    download_all_biomas_data()
