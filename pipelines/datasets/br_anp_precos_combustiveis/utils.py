# -*- coding: utf-8 -*-
"""
General purpose functions for the br_anp_precos_combustiveis project
"""

import os
import requests
from bs4 import BeautifulSoup


def download_files():
    # ! URL da página que contém os links de download
    url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"

    # ! Fazer solicitação GET para a página
    # ? O método get() retorna um objeto Response
    response = requests.get(url)

    # ! Analisar o HTML da página usando a biblioteca BeautifulSoup
    # ? Usando a biblioteca BeautifulSoup para analisar o HTML da página
    # * Primeiro argumento: o conteúdo HTML da página
    # * Segundo argumento: o parser HTML que será usado para analisar o HTML
    soup = BeautifulSoup(response.content, "html.parser")

    # ! Encontrar todos os links de download para arquivos CSV
    # ? Usando o método find_all() para encontrar todos os elementos <a> com o atributo href
    # * Segundo argumento: Uma função lambda que é usada como filtro adicional para encontrar apenas os links que terminam com .csv
    links = soup.find_all("a", href=lambda href: href and href.endswith(".csv"))

    diretorio = "/tmp/input"

    if not os.path.exists(diretorio):
        os.mkdir(diretorio)

    for link in links:
        filename = link.get("href").split("/")[-1]
        file_url = link.get("href")
        response = requests.get(file_url)

        with open(f"input/{filename}", "wb") as f:
            f.write(response.content)

        print(f"Arquivo {filename} baixado com sucesso!")
