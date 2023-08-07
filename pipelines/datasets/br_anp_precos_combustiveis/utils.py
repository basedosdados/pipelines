# -*- coding: utf-8 -*-
"""
General purpose functions for the br_anp_precos_combustiveis project
"""
import basedosdados as bd
import unidecode
import os
import requests
from bs4 import BeautifulSoup
from pipelines.utils.utils import log

def download_files(url: str, path: str):
    # ! URL da página que contém os links de download

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

    if not os.path.exists(path):
        os.mkdir(path)

    for link in links:
        filename = link.get("href").split("/")[-1]
        file_url = link.get("href")
        response = requests.get(file_url)

        with open(f"input/{filename}", "wb") as f:
            f.write(response.content)

        log(f"Arquivo {filename} baixado com sucesso!")

def get_id_municipio():
    # ! Carregando os dados direto do Diretório de municipio da BD
    # Para carregar o dado direto no pandas
    log("Carregando dados do diretório de municípios da BD")
    id_municipio = bd.read_table(
        dataset_id="br_bd_diretorios_brasil",
        table_id="municipio",
        billing_project_id="basedosdados-dev",
        from_file=True,
    )
    log("----" * 150)
    log("Dados carregados com sucesso")
    log("----" * 150)
    log("Iniciando tratamento dos dados id_municipio")
    # ! Tratamento do id_municipio para mergear com a base
    id_municipio["nome"] = id_municipio["nome"].str.upper()
    id_municipio["nome"] = id_municipio["nome"].apply(unidecode.unidecode)
    id_municipio["nome"] = id_municipio["nome"].replace("ESPIGAO D'OESTE", "ESPIGAO DO OESTE")
    id_municipio["nome"] = id_municipio["nome"].replace("SANT'ANA DO LIVRAMENTO", "SANTANA DO LIVRAMENTO")
    id_municipio = id_municipio[["id_municipio", "nome", "sigla_uf"]]

    return id_municipio
