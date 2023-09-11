# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_cnes project
"""

from bs4 import BeautifulSoup
import requests
from datetime import datetime
import os
from pipelines.utils.utils import log
import unicodedata


# função para extrair datas
# valor usado para o check de atualização do site além de ser usado para update do coverage
# tambem será usado para criar uma coluna


def remove_accent(input_str, pattern="all"):
    if not isinstance(input_str, str):
        input_str = str(input_str)

    patterns = set(pattern)

    if "Ç" in patterns:
        patterns.remove("Ç")
        patterns.add("ç")

    symbols = {
        "acute": "áéíóúÁÉÍÓÚýÝ",
        "grave": "àèìòùÀÈÌÒÙ",
        "circunflex": "âêîôûÂÊÎÔÛ",
        "tilde": "ãõÃÕñÑ",
        "umlaut": "äëïöüÄËÏÖÜÿ",
        "cedil": "çÇ",
    }

    nude_symbols = {
        "acute": "aeiouAEIOUyY",
        "grave": "aeiouAEIOU",
        "circunflex": "aeiouAEIOU",
        "tilde": "aoAOnN",
        "umlaut": "aeiouAEIOUy",
        "cedil": "cC",
    }

    accent_types = ["´", "`", "^", "~", "¨", "ç"]

    if any(
        pattern in {"all", "al", "a", "todos", "t", "to", "tod", "todo"}
        for pattern in patterns
    ):
        return "".join(
            [
                c if unicodedata.category(c) != "Mn" else ""
                for c in unicodedata.normalize("NFD", input_str)
            ]
        )

    for p in patterns:
        if p in accent_types:
            input_str = "".join(
                [
                    c if c not in symbols[p] else nude_symbols[p][symbols[p].index(c)]
                    for c in input_str
                ]
            )

    return input_str


def parse_date_parse_files(url):
    xpath_release_date = "tr td:nth-of-type(3)"
    response = requests.get(url)

    # Checa se a requisição foi bem sucedida
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

        ## 1. parsing da data de atualização ##
        # Seleciona tags com base no xpath
        td_elements = soup.select(xpath_release_date)
        # Extrai texto das tags selecionadas
        td_texts = [td.get_text(strip=True) for td in td_elements]
        # selecionar someente data
        td_texts = [td[0:10] for td in td_texts]
        # seleciona data
        # são liberados 20 arquivos a cada atualização, a data é a mesma o que varia é a hora.
        td_texts = td_texts[3]
        # converte para data
        release_data = datetime.strptime(td_texts, "%Y-%m-%d").date()
        log(f"realease date: {release_data}")

        ## 2. parsing dos links de download ##

        a_elements = soup.find_all("a")
        # Extrai todos href
        href_values = [a["href"] for a in a_elements]
        # Filtra href com nomes dos arquivos
        files_name = [href for href in href_values if "PARTE" in href]
        log(f"files name: {files_name}")
        return release_data, files_name
    else:
        log(
            f"Não foi possível acessar o site :/. O status da requisição foi: {response.status_code}"
        )
        return []


def download_csv_files(url, file_name, download_directory):
    # Ensure the download directory exists
    os.makedirs(download_directory, exist_ok=True)

    log(f"Downloading--------- {url}")
    # Extract the file name from the URL

    # Set the full file path for the download
    file_path = os.path.join(download_directory, file_name)

    # Send a GET request to download the file
    response = requests.get(url)

    if response.status_code == 200:
        # Save the file to the specified directory
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded {file_name}")
    else:
        print(f"Failed to download {file_name}. Status code: {response.status_code}")


def preserve_zeros(x):
    return x.strip()


# os arquivos vem num formato sem um separador aparente
# sigla_uf e municipio estao concatenados ex. ROCACOAL ; PAMONTE ALEGRE
# STATUS IMOVEL junto com endereço
# ex. 02MARGEM DIR DO LAGO DO CHUMBO  ; 02ESQUERDA DA RODOVIA AM 010 KM 04

# id sncr tem 9 digitos
# https://sncr.serpro.gov.br/sncr-web/consultaPublica.jsf?windowId=8dc
#
# nr_imovel(8)nr_incra(13)
# 000000510000013030010310393490

# 00000051 000001303 0010310393490
# nome_fazenda
# FAZENDA JUSSARA
# sit_imovel(2) endereço(xxxx)
# 02LT 01 GL 03 PF CO
# zona(xxxx) - as vezes existe as vezes não
# ZONA RURAL
# sigla_uf(2)municipio(xxxx) #converter para id com verificação cruzada com cep
# ROPIMENTA BUENO
# cep(8)date(8)something(3)status_sncr(1)
# 7691900020210331NAO1
