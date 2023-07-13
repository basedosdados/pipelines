# -*- coding: utf-8 -*-
"""
Tasks for mundo_transfermarkt_competicoes
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################
from pipelines.utils.utils import log, to_partitions

# from pipelines.datasets.mundo_transfermarkt_competicoes.decorators import retry
# from pipelines.constants import constants as constants
from pipelines.datasets.mundo_transfermarkt_competicoes.constants import (
    constants as mundo_constants,
)
from prefect import task
import re
from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
import time as tm

# import asyncio

# import datetime
from pipelines.datasets.mundo_transfermarkt_competicoes.utils import (
    process_basico,
    process,
    pegar_valor,
    pegar_valor_sem_tecnico,
    valor_vazio,
    vazio,
)


@task
def execucao_coleta():
    """
    Execute the program
    """
    # Armazena informações do site em um único dataframe.
    base_url = "https://www.transfermarkt.com/campeonato-brasileiro-serie-a/gesamtspielplan/wettbewerb/BRA1?saison_id={season}&spieltagVon=1&spieltagBis=38"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"
    }
    base_link = "https://www.transfermarkt.com"
    links = []

    ht_tag = []
    at_tag = []
    result_tag = []

    ht = []
    at = []
    fthg = []
    ftag = []
    col_home = []
    col_away = []

    # expressão regular para encontrar o número de gols marcados pelo time da casa
    pattern_fthg = re.compile(r"\d:")
    # expressão regular para encontrar o número de gols marcados pelo time visitante
    pattern_ftag = re.compile(r":\d")

    # para armazenar os dados das partidas
    df = pd.DataFrame(
        {"ht": [], "at": [], "fthg": [], "ftag": [], "col_home": [], "col_away": []}
    )
    df_valor = pd.DataFrame({})

    # season = data_atual.year - 1
    season = mundo_constants.SEASON.value
    # Pegar o link das partidas
    # Para cada temporada, adiciona os links dos jogos em `links`
    log(f"Obtendo links: temporada {season}")
    site_data = requests.get(base_url.format(season=season), headers=headers)
    soup = BeautifulSoup(site_data.content, "html.parser")
    link_tags = soup.find_all("a", attrs={"class": "ergebnis-link"})[:10]
    for tag in link_tags:
        links.append(re.sub(r"\s", "", tag["href"]))
    if len(links) % 10 != 0:
        num_excess = len(links) % 10
        del links[-num_excess:]
    tabela = soup.findAll("div", class_="box")
    for i in range(1, int(len(links) / 10) + 1):
        for row in tabela[i].findAll("tr"):  # para tudo que estiver em <tr>
            cells = row.findAll("td")  # variável para encontrar <td>
            if len(cells) == 7:
                ht_tag.append(cells[2].findAll(text=True))  # iterando sobre cada linha
                result_tag.append(
                    cells[4].findAll(text=True)
                )  # iterando sobre cada linha
                at_tag.append(cells[6].findAll(text=True))  # iterando sobre cada linha

    for time in range(len(links)):
        ht.append(str(ht_tag[time][2]))
        col_home.append(str(ht_tag[time][0]))
    for time in range(len(links)):
        at.append(str(at_tag[time][0]))
        col_away.append(str(at_tag[time][2]))
    for tag in result_tag:
        fthg.append(str(pattern_fthg.findall(str(tag))))
        ftag.append(str(pattern_ftag.findall(str(tag))))

    # links das estatísticas
    links_esta = []
    # links das escalações de cada partida
    links_valor = []

    for link in links:
        esta = link.replace("index", "statistik")
        links_esta.append(esta)
    for link in links:
        valor = link.replace("index", "aufstellung")
        links_valor.append(valor)

    n_links = len(links)
    log(f"Encontrados {n_links} partidas.")
    log("Extraindo dados...")
    for n, link in enumerate(links_esta):
        # Tentativas de obter os links
        attempts = 0
        while attempts < 3:
            attempts += 1
            log(f"Attempt {attempts}")
            try:
                link_data = requests.get(base_link + link, headers=headers)
                log("link encontrado!")
                break
            except requests.exceptions.Timeout:
                tm.sleep(3)
        link_soup = BeautifulSoup(link_data.content, "html.parser")
        content = link_soup.find("div", id="main")
        if content:
            try:
                df = process(df, content)
            except Exception:
                try:
                    df = process_basico(df, content)
                except Exception:
                    df = vazio(df)
        else:
            df = vazio(df)
        log(f"{n+1} dados de {n_links} extraídos.")
    for n, link in enumerate(links_valor):
        # Tentativas de obter os links
        attempts = 0
        while attempts < 3:
            attempts += 1
            log(f"Attempt {attempts}")
            try:
                link_data = requests.get(base_link + link, headers=headers)
                log("link encontrado!")
                break
            except requests.exceptions.Timeout:
                tm.sleep(3)
        link_soup = BeautifulSoup(link_data.content, "html.parser")
        content = link_soup.find("div", id="main")

        if content:
            try:
                df_valor = pegar_valor(df_valor, content)
            except Exception:
                try:
                    df_valor = pegar_valor_sem_tecnico(df_valor, content)
                except Exception:
                    df_valor = valor_vazio(df_valor)
        else:
            df_valor = valor_vazio(df_valor)
        log(f"{n+1} valores de {n_links} extraídos.")

    df["ht"] = ht
    df["at"] = at
    df["fthg"] = fthg
    df["ftag"] = ftag
    df["col_home"] = col_home
    df["col_away"] = col_away

    # limpar variaveis
    df["fthg"] = df["fthg"].map(lambda x: x.replace("['", ""))
    df["fthg"] = df["fthg"].map(lambda x: x.replace(":']", ""))

    df["ftag"] = df["ftag"].map(lambda x: x.replace("[':", ""))
    df["ftag"] = df["ftag"].map(lambda x: x.replace("']", ""))

    df["col_home"] = df["col_home"].map(lambda x: x.replace("(", ""))
    df["col_home"] = df["col_home"].map(lambda x: x.replace(".)", ""))

    df["col_away"] = df["col_away"].map(lambda x: x.replace("(", ""))
    df["col_away"] = df["col_away"].map(lambda x: x.replace(".)", ""))

    df["htag"] = df["htag"].map(lambda x: str(x).replace(")", ""))
    df["hthg"] = df["hthg"].map(lambda x: str(x).replace("(", ""))

    df_valor["idade_media_titular_vis"] = df_valor["idade_media_titular_vis"].map(
        lambda x: str(x).replace(" ", "")
    )
    df_valor["idade_media_titular_man"] = df_valor["idade_media_titular_man"].map(
        lambda x: str(x).replace(" ", "")
    )

    df_valor["valor_equipe_titular_man"] = df_valor["valor_equipe_titular_man"].map(
        lambda x: str(x).replace("m", "0000")
    )
    df_valor["valor_equipe_titular_man"] = df_valor["valor_equipe_titular_man"].map(
        lambda x: str(x).replace("Th.", "000")
    )
    df_valor["valor_equipe_titular_man"] = df_valor["valor_equipe_titular_man"].map(
        lambda x: str(x).replace(".", "")
    )

    df_valor["valor_equipe_titular_vis"] = df_valor["valor_equipe_titular_vis"].map(
        lambda x: str(x).replace("m", "0000")
    )
    df_valor["valor_equipe_titular_vis"] = df_valor["valor_equipe_titular_vis"].map(
        lambda x: str(x).replace("Th.", "000")
    )
    df_valor["valor_equipe_titular_vis"] = df_valor["valor_equipe_titular_vis"].map(
        lambda x: str(x).replace(".", "")
    )

    df["publico_max"] = df["publico_max"].map(lambda x: str(x).replace(".", ""))
    df["publico"] = df["publico"].map(lambda x: str(x).replace(".", ""))

    df["test"] = df["publico_max"].replace(to_replace=r"\d", value=1, regex=True)

    def sem_info(x, y):
        if x == 1:
            return y
        return None

    df["test2"] = df.apply(lambda x: sem_info(x["test"], x["publico_max"]), axis=1)
    df["publico_max"] = df["test2"]
    del df["test2"]
    del df["test"]

    df["data"] = pd.to_datetime(df["data"]).dt.date
    df["horario"] = pd.to_datetime(df["horario"]).dt.strftime("%H:%M")

    df.fillna("", inplace=True)

    df["rodada"] = df["rodada"].astype(np.int64)

    # renomear colunas
    df = df.rename(
        columns={
            "ht": "time_man",
            "at": "time_vis",
            "fthg": "gols_man",
            "ftag": "gols_vis",
            "col_home": "colocacao_man",
            "col_away": "colocacao_vis",
            "ac": "escanteios_vis",
            "hc": "escanteios_man",
            "adef": "defesas_vis",
            "hdef": "defesas_man",
            "af": "faltas_vis",
            "afk": "chutes_bola_parada_vis",
            "aimp": "impedimentos_vis",
            "as": "chutes_vis",
            "asofft": "chutes_fora_vis",
            "hf": "faltas_man",
            "hfk": "chutes_bola_parada_man",
            "himp": "impedimentos_man",
            "hs": "chutes_man",
            "hsofft": "chutes_fora_man",
            "htag": "gols_1_tempo_vis",
            "hthg": "gols_1_tempo_man",
        }
    )

    df = pd.concat([df, df_valor], axis=1)

    df["data"] = pd.to_datetime(df["data"])
    df["ano_campeonato"] = mundo_constants.DATA_ATUAL_ANO.value
    # df["ano_campeonato"] = df["data"].dt.year

    df = df[mundo_constants.ORDEM_COLUNA_FINAL.value]

    return df


@task
def make_partitions(df):
    to_partitions(
        data=df,
        partition_columns=["ano_campeonato"],
        savepath="/tmp/data/mundo_transfermarkt_competicoes/output/",
    )
    return "/tmp/data/mundo_transfermarkt_competicoes/output/"
