# -*- coding: utf-8 -*-
"""
General purpose functions for the br_isp_estatisticas_seguranca project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_isp_estatisticas_seguranca.
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
# from pipelines.datasets.br_isp_estatisticas_seguranca.utils import foo
# foo()
# ```
#
###############################################################################

import requests
from bs4 import BeautifulSoup as soup
import pandas as pd

# extract all href inside a tags inside a div found in this xpath //*[@id="conteudo_mostrar"]/div
#  in this url http://www.ispdados.rj.gov.br/estatistica.html


def get_links():
    url = "http://www.ispdados.rj.gov.br/estatistica.html"
    response = requests.get(url)
    page_soup = soup(response.content, "html.parser")
    div = page_soup.find("div", {"id": "conteudo_mostrar"})
    links = []
    for a in div.find_all("a", href=True):
        if a["href"].endswith(".csv") or a["href"].endswith(".xlsx"):
            links.append(a["href"])
    return links
