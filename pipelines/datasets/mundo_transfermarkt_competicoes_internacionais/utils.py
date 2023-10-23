# -*- coding: utf-8 -*-
"""
General purpose functions for the mundo_transfermarkt_competicoes_internacionais project
"""
###############################################################################

import re
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from pipelines.datasets.mundo_transfermarkt_competicoes_internacionais.constants import (
    constants as mundo_constants,
)
from pipelines.datasets.mundo_transfermarkt_competicoes_internacionais.decorators import (
    retry,
)
from pipelines.utils.utils import log


def obter_ano():
    data_atual = datetime.today()
    if data_atual.month > 7:
        return data_atual.year
    else:
        return data_atual.year - 1


def obter_temporada():
    data_atual = datetime.today()

    if data_atual.month > 7:
        primeiro_ano = data_atual.year
        segundo_ano = data_atual.year + 1
    else:
        primeiro_ano = data_atual.year - 1
        segundo_ano = data_atual.year

    return f"{primeiro_ano},{segundo_ano}"


@retry
def get_content(link_soup):
    content = link_soup.find("div", id="main")
    return content


def process(df, content):
    """
    Process complete
    """
    new_content = {
        "time_man": content.find_all("div", attrs={"class": "sb-team sb-heim"})[0]
        .find("a", class_="sb-vereinslink")
        .text,
        "time_vis": content.find_all("div", attrs={"class": "sb-team sb-gast"})[0]
        .find("a", class_="sb-vereinslink")
        .text,
        "gols_man": content.find_all("div", attrs={"class": "sb-endstand"})[0]
        .get_text()
        .strip()
        .split(":")[0],
        "gols_vis": re.search(
            r":(\d+)",
            content.find_all("div", attrs={"class": "sb-endstand"})[0]
            .get_text()
            .strip(),
        ).group(1),
        "estadio": content.find_all("td", attrs={"class": "hauptlink"})[0].get_text(),
        "data": re.search(
            re.compile(r"\d+/\d+/\d+"),
            content.find("a", text=re.compile(r"\d+/\d+/\d")).get_text().strip(),
        ).group(0),
        "horario": content.find_all("p", attrs={"class": "sb-datum hide-for-small"})[0]
        .get_text()
        .split("|")[2]
        .strip(),
        "fase": content.find_all("p", attrs={"class": "sb-datum hide-for-small"})[0]
        .get_text()
        .split("|")[0]
        .strip(),
        "publico": content.find_all("td", attrs={"class": "hauptlink"})[1].get_text(),
        "publico_max": content.find_all("table", attrs={"class": "profilheader"})[0]
        .find_all("td")[2]
        .get_text(),
        "arbitro": content.find_all("table", attrs={"class": "profilheader"})[1]
        .find_all("a")[0]
        .get_text(),
        "arbitro_nacionalidade": content.find_all(
            "table", attrs={"class": "profilheader"}
        )[1]
        .find_all("a")[1]
        .get_text(),
        "gols_1_tempo": content.find_all("div", attrs={"class": "sb-halbzeit"})[0]
        .get_text()
        .split()[0],
        "chutes_man": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            0
        ].get_text(),
        "chutes_vis": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            1
        ].get_text(),
        "chutes_fora_man": content.find_all(
            "div", attrs={"class": "sb-statistik-zahl"}
        )[2].get_text(),
        "chutes_fora_vis": content.find_all(
            "div", attrs={"class": "sb-statistik-zahl"}
        )[3].get_text(),
        "defesas_man": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            4
        ].get_text(),
        "defesas_vis": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            5
        ].get_text(),
        "faltas_man": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            10
        ].get_text(),
        "faltas_vis": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            11
        ].get_text(),
        "escanteios_man": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            6
        ].get_text(),
        "escanteios_vis": content.find_all("div", attrs={"class": "sb-statistik-zahl"})[
            7
        ].get_text(),
        "impedimentos_man": content.find_all(
            "div", attrs={"class": "sb-statistik-zahl"}
        )[12].get_text(),
        "impedimentos_vis": content.find_all(
            "div", attrs={"class": "sb-statistik-zahl"}
        )[13].get_text(),
        "chutes_bola_parada_man": content.find_all(
            "div", attrs={"class": "sb-statistik-zahl"}
        )[8].get_text(),
        "chutes_bola_parada_vis": content.find_all(
            "div", attrs={"class": "sb-statistik-zahl"}
        )[9].get_text(),
    }
    df = pd.concat([df, pd.DataFrame([new_content])], ignore_index=True)
    return df


def process_basico(df, content):
    """
    Process data
    """
    new_content = {
        "time_man": content.find_all("div", attrs={"class": "sb-team sb-heim"})[0]
        .find("a", class_="sb-vereinslink")
        .text,
        "time_vis": content.find_all("div", attrs={"class": "sb-team sb-gast"})[0]
        .find("a", class_="sb-vereinslink")
        .text,
        "gols_man": content.find_all("div", attrs={"class": "sb-endstand"})[0]
        .get_text()
        .strip()
        .split(":")[0],
        "gols_vis": re.search(
            r":(\d+)",
            content.find_all("div", attrs={"class": "sb-endstand"})[0]
            .get_text()
            .strip(),
        ).group(1),
        "estadio": content.find_all("td", attrs={"class": "hauptlink"})[0].get_text(),
        "data": re.search(
            re.compile(r"\d+/\d+/\d+"),
            content.find("a", text=re.compile(r"\d+/\d+/\d")).get_text().strip(),
        ).group(0),
        "horario": content.find_all("p", attrs={"class": "sb-datum hide-for-small"})[0]
        .get_text()
        .split("|")[2]
        .strip(),
        "fase": content.find_all("p", attrs={"class": "sb-datum hide-for-small"})[0]
        .get_text()
        .split("|")[0]
        .strip(),
        "publico": content.find_all("td", attrs={"class": "hauptlink"})[1].get_text(),
        "publico_max": content.find_all("table", attrs={"class": "profilheader"})[0]
        .find_all("td")[2]
        .get_text(),
        "arbitro": content.find_all("table", attrs={"class": "profilheader"})[1]
        .find_all("a")[0]
        .get_text(),
        "arbitro_nacionalidade": content.find_all(
            "table", attrs={"class": "profilheader"}
        )[1]
        .find_all("a")[1]
        .get_text(),
        "gols_1_tempo": content.find_all("div", attrs={"class": "sb-halbzeit"})[0]
        .get_text()
        .split()[0],
        "chutes_man": None,
        "chutes_vis": None,
        "chutes_fora_man": None,
        "chutes_fora_vis": None,
        "defesas_man": None,
        "defesas_vis": None,
        "faltas_man": None,
        "faltas_vis": None,
        "escanteios_man": None,
        "escanteios_vis": None,
        "impedimentos_man": None,
        "impedimentos_vis": None,
        "chutes_bola_parada_man": None,
        "chutes_bola_parada_vis": None,
    }
    df = pd.concat([df, pd.DataFrame([new_content])], ignore_index=True)
    return df


def vazio(df):
    """
    Return a template DataFrame
    """
    new_content = {
        "time_man": None,
        "time_vis": None,
        "gols_man": None,
        "gols_vis": None,
        "estadio": None,
        "data": None,
        "horario": None,
        "fase": None,
        "publico": None,
        "publico_max": None,
        "arbitro": None,
        "arbitro_nacionalidade": None,
        "gols_1_tempo": None,
        "chutes_man": None,
        "chutes_vis": None,
        "chutes_fora_man": None,
        "chutes_fora_vis": None,
        "defesas_man": None,
        "defesas_vis": None,
        "faltas_man": None,
        "faltas_vis": None,
        "escanteios_man": None,
        "escanteios_vis": None,
        "impedimentos_man": None,
        "impedimentos_vis": None,
        "chutes_bola_parada_man": None,
        "chutes_bola_parada_vis": None,
    }
    df = pd.concat([df, pd.DataFrame([new_content])], ignore_index=True)
    return df


def process_gols(value):
    if value is None:
        return [None, None, None, None]
    if "prorr." in value:
        return [None, None, 1, 0]
    elif "pen." in value:
        return [None, None, None, 1]
    else:
        man, vis = value.strip("()").split(":")
        return [man, vis, 0, 0]


def formatar_valor(valor):
    valor = str(valor)
    valor = valor.replace(" mi.", "0000")
    valor = valor.replace(" bilhões", "0000000")
    valor = valor.replace(",", "")
    return valor


# Função para formatar as datas
def formatar_data(data):
    try:
        # Tentar o formato "%d/%m/%Y"
        return pd.to_datetime(data, format="%d/%m/%Y").dt.date
    except ValueError:
        try:
            # Tentar o formato "%d/%m/%y"
            return pd.to_datetime(data, format="%d/%m/%y").dt.date
        except ValueError:
            # Se nenhum dos formatos funcionar, retornar NaN
            return pd.NaT


def definir_tipo_fase(row):
    if row is not None:
        if row.startswith("Grupo "):
            return row
        elif "-" in row:
            partes = row.split(" - ")
            return partes[0]
        else:
            return "Jogo único"
    else:
        return None


def definir_fase(row):
    if row is not None:
        if row.startswith("Grupo "):
            return "Fase de grupos"
        elif "-" in row:
            partes = row.split(" - ")
            return partes[0]
        else:
            return row
    else:
        return None


def pegar_valor(df, content):
    """
    Get value
    """
    # gera um dicionário
    valor_content = {
        "valor_equipe_titular_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[0]
        .text.split("€", 1)[0],
        "valor_equipe_titular_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[1]
        .text.split("€", 1)[0],
        "valor_medio_equipe_titular_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[2]
        .text.split("€", 1)[0],
        "valor_medio_equipe_titular_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[3]
        .text.split("€", 1)[0],
        "idade_media_titular_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[4]
        .text.split()[0],
        "idade_media_titular_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[5]
        .text.split()[0],
        "convocacao_selecao_principal_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[6]
        .text.split()[0],
        "convocacao_selecao_principal_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[7]
        .text.split()[0],
        "selecao_juniores_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[8]
        .text.split()[0],
        "selecao_juniores_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[9]
        .text.split()[0],
        "estrangeiros_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[10]
        .text.split()[0],
        "estrangeiros_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[11]
        .text.split()[0],
        "socios_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[12]
        .text.split()[0],
        "socios_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[13]
        .text.split()[0],
        "tecnico_man": content.find_all("div", class_="container-inhalt")[0]
        .find("a")
        .text.strip(),
        "tecnico_vis": content.find_all("div", class_="container-inhalt")[1]
        .find("a")
        .text.strip(),
        "idade_tecnico_man": content.find_all("div", class_="container-inhalt")[0]
        .find("b", text="Idade:")
        .nextSibling.strip()
        .split()[0],
        "idade_tecnico_vis": content.find_all("div", class_="container-inhalt")[1]
        .find("b", text="Idade:")
        .nextSibling.strip()
        .split()[0],
        "data_inicio_tecnico_man": content.find_all("div", class_="container-inhalt")[0]
        .find("b", text="Desde:")
        .nextSibling.strip(),
        "data_inicio_tecnico_vis": content.find_all("div", class_="container-inhalt")[1]
        .find("b", text="Desde:")
        .nextSibling.strip(),
        "data_final_tecnico_man": content.find_all("div", class_="container-inhalt")[0]
        .find("b", text="Contrato até:")
        .nextSibling.strip(),
        "data_final_tecnico_vis": content.find_all("div", class_="container-inhalt")[1]
        .find("b", text="Contrato até:")
        .nextSibling.strip(),
        "proporcao_sucesso_man": content.find_all("div", class_="container-inhalt")[0]
        .find("b", text="Proporção de sucesso:")
        .nextSibling.strip(),
        "proporcao_sucesso_vis": content.find_all("div", class_="container-inhalt")[1]
        .find("b", text="Proporção de sucesso:")
        .nextSibling.strip(),
    }
    df = pd.concat([df, pd.DataFrame([valor_content])], ignore_index=True)
    return df


def pegar_valor_sem_tecnico(df, content):
    """
    Get value without technical
    """
    valor_content = {
        "valor_equipe_titular_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[0]
        .text.split("€", 1)[0],
        "valor_equipe_titular_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[1]
        .text.split("€", 1)[0],
        "valor_medio_equipe_titular_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[2]
        .text.split("€", 1)[0],
        "valor_medio_equipe_titular_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[3]
        .text.split("€", 1)[0],
        "idade_media_titular_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[4]
        .text.split()[0],
        "idade_media_titular_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[5]
        .text.split()[0],
        "convocacao_selecao_principal_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[6]
        .text.split()[0],
        "convocacao_selecao_principal_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[7]
        .text.split()[0],
        "selecao_juniores_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[8]
        .text.split()[0],
        "selecao_juniores_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[9]
        .text.split()[0],
        "estrangeiros_man": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[10]
        .text.split()[0],
        "estrangeiros_vis": content.findAll("tbody")[1]
        .find_all("span", class_="datenundfakten_bar")[11]
        .text.split()[0],
        "socios_man": None,
        "socios_vis": None,
        "tecnico_man": None,
        "tecnico_vis": None,
        "idade_tecnico_man": None,
        "idade_tecnico_vis": None,
        "data_inicio_tecnico_man": None,
        "data_inicio_tecnico_vis": None,
        "data_final_tecnico_man": None,
        "data_final_tecnico_vis": None,
        "proporcao_sucesso_man": None,
        "proporcao_sucesso_vis": None,
    }
    df = pd.concat([df, pd.DataFrame([valor_content])], ignore_index=True)
    return df


def valor_vazio(df):
    """
    Return a temmplate DataFrame
    """
    valor_content = {
        "valor_equipe_titular_man": None,
        "valor_equipe_titular_vis": None,
        "valor_medio_equipe_titular_man": None,
        "valor_medio_equipe_titular_vis": None,
        "idade_media_titular_man": None,
        "idade_media_titular_vis": None,
        "convocacao_selecao_principal_man": None,
        "convocacao_selecao_principal_vis": None,
        "selecao_juniores_man": None,
        "selecao_juniores_vis": None,
        "estrangeiros_man": None,
        "estrangeiros_vis": None,
        "socios_man": None,
        "socios_vis": None,
        "tecnico_man": None,
        "tecnico_vis": None,
        "idade_tecnico_man": None,
        "idade_tecnico_vis": None,
        "data_inicio_tecnico_man": None,
        "data_inicio_tecnico_vis": None,
        "data_final_tecnico_man": None,
        "data_final_tecnico_vis": None,
        "proporcao_sucesso_man": None,
        "proporcao_sucesso_vis": None,
    }
    df = pd.concat([df, pd.DataFrame([valor_content])], ignore_index=True)
    return df


def data_url():
    base_url = "https://www.transfermarkt.com.br/uefa-champions-league/gesamtspielplan/pokalwettbewerb/CL/saison_id/{season}"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"
    }
    base_link_br = "https://www.transfermarkt.com.br"

    links = []
    season = obter_ano()

    print(f"Obtendo links: temporada {season}")
    site_data = requests.get(base_url.format(season=season), headers=headers)
    soup = BeautifulSoup(site_data.content, "html.parser")
    link_tags = soup.find_all("a", attrs={"class": "ergebnis-link"})
    for tag in link_tags:
        links.append(re.sub(r"\s", "", tag["href"]))

    link_data = requests.get(base_link_br + links[-1], headers=headers)
    link_soup = BeautifulSoup(link_data.content, "html.parser")
    content = link_soup.find("div", id="main")
    data = re.search(
        re.compile(r"\d+/\d+/\d+"),
        content.find("a", text=re.compile(r"\d+/\d+/\d")).get_text().strip(),
    ).group(0)
    # Converter a data para um objeto de data
    data_obj = datetime.strptime(data, "%d/%m/%y")

    return data_obj


async def execucao_coleta():
    base_url = "https://www.transfermarkt.com.br/uefa-champions-league/gesamtspielplan/pokalwettbewerb/CL/saison_id/{season}"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"
    }

    base_link_br = "https://www.transfermarkt.com.br"

    links = []
    season = obter_ano()
    log(f"Obtendo links: temporada {season}")
    site_data = requests.get(base_url.format(season=season), headers=headers)
    soup = BeautifulSoup(site_data.content, "html.parser")
    link_tags = soup.find_all("a", attrs={"class": "ergebnis-link"})
    for tag in link_tags:
        links.append(re.sub(r"\s", "", tag["href"]))

    # links das estatísticas
    links_esta = []
    # links das escalações de cada partida
    links_valor = []

    for link in links:
        esta = link.replace("index", "statistik")
        links_esta.append(esta)
    for link in links:
        valor = link.replace("index", "vorbericht")
        links_valor.append(valor)

    n_links = len(links)
    df = pd.DataFrame({})
    for n, link in enumerate(links_esta):
        content = await get_content(base_link_br + link, wait_time=0.01)
        # link_data = requests.get(base_link_br + link, headers=headers)
        # link_soup = BeautifulSoup(link_data.content, "html.parser")
        # content = link_soup.find("div", id="main")
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
        log(f"{n+1} dados sobre estatística de {n_links} extraídos.")

    df_valor = pd.DataFrame({})
    for n, link in enumerate(links_valor):
        content = await get_content(base_link_br + link, wait_time=0.01)
        # link_data = requests.get(base_link_br + link, headers=headers)
        # link_soup = BeautifulSoup(link_data.content, "html.parser")
        # content = link_soup.find("div", id="main")

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

    # Tratamento
    ## df_valor tratando
    colunas_formatar = [
        "valor_equipe_titular_man",
        "valor_equipe_titular_vis",
        "valor_medio_equipe_titular_man",
        "valor_medio_equipe_titular_vis",
    ]

    for coluna in colunas_formatar:
        df_valor[coluna] = df_valor[coluna].map(formatar_valor)

    df_valor["proporcao_sucesso_man"] = df_valor["proporcao_sucesso_man"].map(
        lambda x: str(x).replace(" PPj", "")
    )
    df_valor["proporcao_sucesso_vis"] = df_valor["proporcao_sucesso_vis"].map(
        lambda x: str(x).replace(" PPj", "")
    )

    df_valor["socios_man"] = df_valor["socios_man"].map(
        lambda x: str(x).replace("?", "")
    )
    df_valor["socios_vis"] = df_valor["socios_vis"].map(
        lambda x: str(x).replace("?", "")
    )

    # Aplicar a função às colunas desejadas
    colunas_formatar = [
        "data_inicio_tecnico_man",
        "data_inicio_tecnico_vis",
    ]

    for coluna in colunas_formatar:
        df_valor[coluna] = formatar_data(df_valor[coluna])

    df_valor["data_final_tecnico_vis"] = df_valor["data_final_tecnico_vis"].replace(
        "-", pd.NaT
    )
    df_valor["data_final_tecnico_man"] = df_valor["data_final_tecnico_man"].replace(
        "-", pd.NaT
    )
    # Em seguida, converta a coluna para o formato datetime
    df_valor["data_final_tecnico_vis"] = pd.to_datetime(
        df_valor["data_final_tecnico_vis"], format="%d/%m/%Y", errors="coerce"
    )
    df_valor["data_final_tecnico_man"] = pd.to_datetime(
        df_valor["data_final_tecnico_man"], format="%d/%m/%Y", errors="coerce"
    )
    ## df tratamento
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%y").dt.date
    df["horario"] = pd.to_datetime(df["horario"], format="%H:%M").dt.strftime("%H:%M")

    for index, row in df.iterrows():
        publico = row["publico"]
        if (
            publico is not None
            and isinstance(publico, str)
            and publico.endswith(" esgotado")
        ):
            valor = publico.replace(" esgotado", "")
            df.at[index, "publico"] = valor

    df["tipo_fase"] = df["fase"].apply(definir_tipo_fase)
    df["fase"] = df["fase"].apply(definir_fase)

    df["publico_max"] = df["publico_max"].map(lambda x: str(x).replace(".", ""))
    df["publico"] = df["publico"].map(lambda x: str(x).replace(".", ""))
    df["publico_max"] = df["publico_max"].str.replace("\n", "")

    df[["gols_1_tempo_man", "gols_1_tempo_vis", "prorrogacao", "penalti"]] = (
        df["gols_1_tempo"].apply(process_gols).apply(pd.Series)
    )

    df = pd.concat([df, df_valor], axis=1)
    df.fillna("", inplace=True)
    df["temporada"] = obter_temporada()
    df = df.loc[:, mundo_constants.ORDEM_COLUNA_FINAL.value]

    return df
