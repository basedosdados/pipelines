# -*- coding: utf-8 -*-
"""
General purpose functions for the mundo_transfermarkt_competicoes project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto mundo_transfermarkt_competicoes.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar funções, basta fazer em código Python comum, como abaixo:
#
# ```
# def foo():
#     """
#     Function foo
#     """
#     print("foo")
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.mundo_transfermarkt_competicoes.utils import foo
# foo()
# ```
#
###############################################################################
import re
from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd


def process_basico(df, content):
    """
    Process data
    """
    # armazenar as informações extraídas do conteúdo HTML.
    # Cada chave do dicionário representa um atributo e seu valor corresponde ao valor extraído do HTML.
    new_content = {
        # Extrai o nome do estádio do HTML
        "estadio": content.find_all("td", attrs={"class": "hauptlink"})[0].get_text(),
        # Extrai a data do HTML.
        # Usa expressões regulares para procurar um padrão de data (no formato dd/mm/aaaa) no texto do link que corresponda ao padrão.
        "data": re.search(
            re.compile(r"\d+/\d+/\d+"),
            content.find("a", text=re.compile(r"\d+/\d+/\d")).get_text().strip(),
        ).group(0),
        # Extrai o horário do HTML.
        "horario": content.find_all("p", attrs={"class": "sb-datum hide-for-small"})[0]
        .get_text()
        .split()[6],
        # Extrai o número da rodada do HTML
        "rodada": re.search(
            re.compile(r"\d+. Matchday"),
            content.find("a", text=re.compile(r"\d.")).get_text().strip(),
        )
        .group(0)
        .split(".", 1)[0],
        # Extrai o número de público do HTML.
        # Procura por todas as tags <td> e obtém o texto da 12ª ocorrência (índice 11).
        "publico": content.find_all("td")[11].get_text(),
        # Extrai o número máximo de público do HTML.
        "publico_max": content.find_all("table", attrs={"class": "profilheader"})[0]
        .find_all("td")[2]
        .get_text(),
        # Extrai o nome do árbitro do HTML.
        "arbitro": re.search(
            r"Referee: [\w\s?]+", content.find_all("p")[2].get_text().strip()
        )
        .group(0)
        .split(": ", 1)[1],
        "hthg": content.find_all("div", attrs={"class": "sb-halbzeit"})[0]
        .get_text()
        .split(":", 1)[0],
        "htag": content.find_all("div", attrs={"class": "sb-halbzeit"})[0]
        .get_text()
        .split(":", 1)[1],
        "hs": None,
        "as": None,
        "hsofft": None,
        "asofft": None,
        "hdef": None,
        "adef": None,
        "hf": None,
        "af": None,
        "hc": None,
        "ac": None,
        "himp": None,
        "aimp": None,
        "hfk": None,
        "afk": None,
    }

    df = df.append(new_content, ignore_index=True)
    return df


def process(df, content):
    """
    Process complete
    """
    new_content = {
        "estadio": content.find_all("td", attrs={"class": "hauptlink"})[0].get_text(),
        "data": re.search(
            re.compile(r"\d+/\d+/\d+"),
            content.find("a", text=re.compile(r"\d+/\d+/\d")).get_text().strip(),
        ).group(0),
        "horario": content.find_all("p", attrs={"class": "sb-datum hide-for-small"})[0]
        .get_text()
        .split()[6],
        "rodada": re.search(
            re.compile(r"\d+. Matchday"),
            content.find("a", text=re.compile(r"\d.")).get_text().strip(),
        )
        .group(0)
        .split(".", 1)[0],
        "publico": content.find_all("td")[11].get_text(),
        "publico_max": content.find_all("table", attrs={"class": "profilheader"})[0]
        .find_all("td")[2]
        .get_text(),
        "arbitro": re.search(
            r"Referee: [\w\s?]+", content.find_all("p")[2].get_text().strip()
        )
        .group(0)
        .split(": ", 1)[1],
        "hthg": content.find_all("div", attrs={"class": "sb-halbzeit"})[0]
        .get_text()
        .split(":", 1)[0],
        "htag": content.find_all("div", attrs={"class": "sb-halbzeit"})[0]
        .get_text()
        .split(":", 1)[1],
        "hs": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            0
        ].get_text(),
        "as": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            1
        ].get_text(),
        "hsofft": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            2
        ].get_text(),
        "asofft": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            3
        ].get_text(),
        "hdef": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            4
        ].get_text(),
        "adef": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            5
        ].get_text(),
        "hf": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            10
        ].get_text(),
        "af": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            11
        ].get_text(),
        "hc": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            6
        ].get_text(),
        "ac": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            7
        ].get_text(),
        "himp": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            12
        ].get_text(),
        "aimp": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            13
        ].get_text(),
        "hfk": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            8
        ].get_text(),
        "afk": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            9
        ].get_text(),
    }

    df = df.append(new_content, ignore_index=True)
    return df


def pegar_valor(df, content):
    """
    Get value
    """
    # gera um dicionário
    valor_content = {
        "valor_equipe_titular_man": content.find_all("div", class_="table-footer")[0]
        .find_all("td")[3]
        .get_text()
        .split("€", 1)[1],
        "valor_equipe_titular_vis": content.find_all("div", class_="table-footer")[1]
        .find_all("td")[3]
        .get_text()
        .split("€", 1)[1],
        "idade_media_titular_man": content.find_all("div", class_="table-footer")[0]
        .find_all("td")[1]
        .get_text()
        .split(":", 1)[1],
        "idade_media_titular_vis": content.find_all("div", class_="table-footer")[1]
        .find_all("td")[1]
        .get_text()
        .split(":", 1)[1],
        "tecnico_man": content.find_all("a", attrs={"id": "0"})[1].get_text(),
        "tecnico_vis": content.find_all("a", attrs={"id": "0"})[3].get_text(),
    }
    df = df.append(valor_content, ignore_index=True)
    return df


def pegar_valor_sem_tecnico(df, content):
    """
    Get value without technical
    """
    valor_content = {
        "valor_equipe_titular_man": content.find_all("div", class_="table-footer")[0]
        .find_all("td")[3]
        .get_text()
        .split("€", 1)[1],
        "valor_equipe_titular_vis": content.find_all("div", class_="table-footer")[1]
        .find_all("td")[3]
        .get_text()
        .split("€", 1)[1],
        "idade_media_titular_man": content.find_all("div", class_="table-footer")[0]
        .find_all("td")[1]
        .get_text()
        .split(":", 1)[1],
        "idade_media_titular_vis": content.find_all("div", class_="table-footer")[1]
        .find_all("td")[1]
        .get_text()
        .split(":", 1)[1],
        "tecnico_man": None,
        "tecnico_vis": None,
    }
    df = df.append(valor_content, ignore_index=True)
    return df


def valor_vazio(df):
    """
    Return a temmplate DataFrame
    """
    valor_content = {
        "valor_equipe_titular_man": None,
        "valor_equipe_titular_vis": None,
        "idade_media_titular_man": None,
        "idade_media_titular_vis": None,
        "tecnico_man": None,
        "tecnico_vis": None,
    }
    df = df.append(valor_content, ignore_index=True)
    return df


def vazio(df):
    """
    Return a template DataFrame
    """
    null_content = {
        "estadio": None,
        "data": None,
        "horario": None,
        "rodada": None,
        "publico": None,
        "publico_max": None,
        "arbitro": None,
        "hthg": None,
        "htag": None,
        "hs": None,
        "as": None,
        "hsofft": None,
        "asofft": None,
        "hdef": None,
        "adef": None,
        "hf": None,
        "af": None,
        "hc": None,
        "ac": None,
        "himp": None,
        "aimp": None,
        "hfk": None,
        "afk": None,
    }
    df = df.append(null_content, ignore_index=True)
    return df
