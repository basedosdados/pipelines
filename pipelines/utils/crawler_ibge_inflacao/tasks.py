# -*- coding: utf-8 -*-
"""
Tasks for br_ibge_inpc
"""
import errno

# pylint: disable=line-too-long, W0702, E1101, W0212,unnecessary-dunder-call,invalid-name,too-many-statements
import glob
import os
import ssl
from datetime import datetime as dt
from time import sleep

import basedosdados as bd
import pandas as pd
import wget
from prefect import task
from tqdm import tqdm

from pipelines.utils.crawler_ibge_inflacao.utils import (
    extract_last_date,
    get_legacy_session,
)
from pipelines.utils.utils import log

# necessary for use wget, see: https://stackoverflow.com/questions/35569042/ssl-certificate-verify-failed-with-python3
ssl._create_default_https_context = ssl._create_unverified_context
# pylint: disable=C0206
# pylint: disable=C0201
# pylint: disable=R0914
# https://sidra.ibge.gov.br/tabela/7062
# https://sidra.ibge.gov.br/tabela/7063
# https://sidra.ibge.gov.br/tabela/7060


@task
def check_for_updates(
    indice: str,
    table_id: str,
    dataset_id: str,
) -> bool:
    """
    Crawler para checar atualizações nas dos conjuntos br_ibge_inpc; br_ibge_ipca; br_ibge_ipca15

    indice: inpc | ipca | ip15
    """

    n_mes = {
        "janeiro": "1",
        "fevereiro": "2",
        "março": "3",
        "abril": "4",
        "maio": "5",
        "junho": "6",
        "julho": "7",
        "agosto": "8",
        "setembro": "9",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12",
    }

    if indice not in ["inpc", "ipca", "ip15"]:
        raise ValueError(
            "indice argument must be one of the following: 'inpc', 'ipca', 'ip15'"
        )

    log(f"Checking for updates in {indice} index for {dataset_id}.{table_id}")

    links = {
        "ipca": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n1/all/v/all/p/last%201/c315/7169/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "inpc": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n1/all/v/all/p/last%201/c315/7169/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "ip15": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n1/all/v/all/p/last%201/c315/7169/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
    }

    links = {k: v for k, v in links.items() if k.__contains__(indice)}

    links_keys = list(links.keys())
    log(links_keys)
    success_dwnl = []

    os.system('mkdir -p "/tmp/check_for_updates/"')
    #
    for key in tqdm(links_keys):
        try:
            response = get_legacy_session().get(links[key])
            # download the csv
            with open(f"/tmp/check_for_updates/{key}.csv", "wb") as f:
                f.write(response.content)
            success_dwnl.append(key)
            sleep(5)
        except Exception as e:
            log(e)
            try:
                sleep(5)
                response = get_legacy_session().get(links[key])
                # download the csv
                with open(f"/tmp/check_for_updates/{key}.csv", "wb") as f:
                    f.write(response.content)
                success_dwnl.append(key)
            except Exception as e:  # pylint: disable=redefined-outer-name
                log(e)

    log(f"success_dwnl: {success_dwnl}")
    if len(links_keys) == len(success_dwnl):
        log("All files were successfully downloaded")

    # quebra o flow se houver erro no download de um arquivo.
    else:
        rems = set(links_keys) - set(success_dwnl)
        log(f"The file was not downloaded {rems}")

    file_name = os.listdir("/tmp/check_for_updates")
    file_path = "/tmp/check_for_updates/" + file_name[0]

    dataframe = pd.read_csv(file_path, skiprows=2, skipfooter=14, sep=";")

    dataframe = dataframe[["Mês"]]

    dataframe[["mes", "ano"]] = dataframe["Mês"].str.split(pat=" ", n=1, expand=True)

    dataframe["mes"] = dataframe["mes"].map(n_mes)

    dataframe = dataframe["ano"][0] + "-" + dataframe["mes"][0]

    dataframe = dt.strptime(dataframe, "%Y-%m")

    max_date_ibge = dataframe.strftime("%Y-%m")

    log(f"A data mais no site do ---IBGE--- para a tabela {indice} é : {max_date_ibge}")
    #  TROCAR PARA BSEDOSDADOS ANTES DE IR PRA PROD
    max_date_bd = extract_last_date(
        dataset_id=dataset_id, table_id=table_id, billing_project_id="basedosdados-dev"
    )
    log(f"A data mais recente da tabela no --- Big Query --- é: {max_date_bd}")
    if max_date_ibge > max_date_bd:
        log(
            f"A tabela {indice} foi atualizada no site do IBGE. O Flow de atualização será executado!"
        )
        return True, str(max_date_ibge)
    else:
        log(
            f"A tabela {indice} não foi atualizada no site do IBGE. O Flow de atualização não será executado!"
        )
        return False, str(max_date_ibge)


@task
def crawler(indice: str, folder: str) -> bool:
    """
    Crawler for IBGE Inflacao

    indice: inpc | ipca | ip15
    folder: br | rm | mun | mes
    """
    if folder not in ["br", "rm", "mun", "mes"]:
        raise ValueError(
            "folder argument must be one of the following: 'br', 'rm', 'mun', 'mes'"
        )

    if indice not in ["inpc", "ipca", "ip15"]:
        raise ValueError(
            "indice argument must be one of the following: 'inpc', 'ipca', 'ip15'"
        )

    log(f"Crawling {indice}")
    os.system("[ -e /tmp/data/input/ ] && rm -r /tmp/data/input/")
    os.system("[ -e /tmp/data/output/ ] && rm -r /tmp/data/output/")
    os.system('mkdir -p "/tmp/data"')
    os.system('mkdir -p "/tmp/data/input"')
    os.system('mkdir -p "/tmp/data/input/br"')
    os.system('mkdir -p "/tmp/data/input/rm"')
    os.system('mkdir -p "/tmp/data/input/mun"')
    os.system('mkdir -p "/tmp/data/input/mes"')
    os.system('mkdir -p "/tmp/data/output"')
    os.system('mkdir -p "/tmp/data/output/ip15"')
    os.system('mkdir -p "/tmp/data/output/ipca"')
    os.system('mkdir -p "/tmp/data/output/inpc"')
    log(os.system("tree /tmp/data"))
    links = {
        "br/ipca_grupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n1/all/v/all/p/all/c315/7170,7445,7486,7558,7625,7660,7712,7766,7786/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "br/inpc_grupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n1/all/v/all/p/all/c315/7170,7445,7486,7558,7625,7660,7712,7766,7786/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "br/ip15_grupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n1/all/v/all/p/all/c315/7170,7445,7486,7558,7625,7660,7712,7766,7786/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "br/ipca_subgrupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n1/all/v/all/p/all/c315/7171,7432,7446,7479,7487,7521,7548,7559,7604,7615,7620,7626,7661,7683,7697,7713,7767,7787,47656/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "br/inpc_subgrupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n1/all/v/all/p/all/c315/7171,7432,7446,7479,7487,7521,7548,7559,7604,7615,7620,7626,7661,7683,7697,7713,7767,7787,47656/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "br/ip15_subgrupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n1/all/v/all/p/all/c315/7171,7432,7446,7479,7487,7521,7548,7559,7604,7615,7620,7626,7661,7683,7697,7713,7767,7787,47656/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "br/ipca_item": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n1/all/v/all/p/all/c315/7172,7184,7200,7219,7241,7254,7283,7303,7335,7349,7356,7372,7384,7389,7401,7415,7433,7447,7454,7461,7480,7484,7488,7495,7517,7522,7541,7549,7560,7572,7587,7605,7616,7621,7627,7640,7656,7662,7684,7690,7695,7698,7714,7730,7758,7777,7782,7788,12427,107678,109464/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "br/inpc_item": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n1/all/v/all/p/all/c315/7172,7184,7200,7219,7241,7254,7283,7303,7335,7349,7356,7372,7384,7389,7401,7415,7433,7447,7454,7461,7480,7484,7488,7495,7517,7522,7541,7549,7560,7572,7587,7605,7616,7621,7627,7640,7656,7662,7684,7690,7695,7698,7714,7730,7758,7777,7782,7788,12427,107678,109464/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "br/ip15_item": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n1/all/v/all/p/all/c315/7172,7184,7200,7219,7241,7254,7283,7303,7335,7349,7356,7372,7384,7389,7401,7415,7433,7447,7454,7461,7480,7484,7488,7495,7517,7522,7541,7549,7560,7572,7587,7605,7616,7621,7627,7640,7656,7662,7684,7690,7695,7698,7714,7730,7758,7777,7782,7788,12427,107678,109464/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "br/ipca_subitem": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/947075621",
        "br/inpc_subitem": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/1084704443",
        "br/ip15_subitem": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-783617233",
        "br/ipca_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n1/all/v/all/p/all/c315/7169/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "br/inpc_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n1/all/v/all/p/all/c315/7169/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "br/ip15_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n1/all/v/all/p/all/c315/7169/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "rm/ipca_grupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n7/all/v/all/p/all/c315/7170,7445,7486,7558,7625,7660,7712,7766,7786/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "rm/inpc_grupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n7/all/v/all/p/all/c315/7170,7445,7486,7558,7625,7660,7712,7766,7786/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "rm/ip15_grupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n7/all/v/all/p/all/c315/7170,7445,7486,7558,7625,7660,7712,7766,7786/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "rm/ipca_subgrupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n7/all/v/all/p/all/c315/7171,7432,7446,7479,7487,7521,7548,7559,7604,7615,7620,7626,7661,7683,7697,7713,7767,7787,47656/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "rm/inpc_subgrupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n7/all/v/all/p/all/c315/7171,7432,7446,7479,7487,7521,7548,7559,7604,7615,7620,7626,7661,7683,7697,7713,7767,7787,47656/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "rm/ip15_subgrupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n7/all/v/all/p/all/c315/7171,7432,7446,7479,7487,7521,7548,7559,7604,7615,7620,7626,7661,7683,7697,7713,7767,7787,47656/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "rm/ipca_item": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n7/all/v/all/p/all/c315/7172,7184,7200,7219,7241,7254,7283,7303,7335,7349,7356,7372,7384,7389,7401,7415,7433,7447,7454,7461,7480,7484,7488,7495,7517,7522,7541,7549,7560,7572,7587,7605,7616,7621,7627,7640,7656,7662,7684,7690,7695,7698,7714,7730,7758,7777,7782,7788,12427,107678,109464/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "rm/inpc_item": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n7/all/v/all/p/all/c315/7172,7184,7200,7219,7241,7254,7283,7303,7335,7349,7356,7372,7384,7389,7401,7415,7433,7447,7454,7461,7480,7484,7488,7495,7517,7522,7541,7549,7560,7572,7587,7605,7616,7621,7627,7640,7656,7662,7684,7690,7695,7698,7714,7730,7758,7777,7782,7788,12427,107678,109464/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "rm/ip15_item": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n7/all/v/all/p/all/c315/7172,7184,7200,7219,7241,7254,7283,7303,7335,7349,7356,7372,7384,7389,7401,7415,7433,7447,7454,7461,7480,7484,7488,7495,7517,7522,7541,7549,7560,7572,7587,7605,7616,7621,7627,7640,7656,7662,7684,7690,7695,7698,7714,7730,7758,7777,7782,7788,12427,107678,109464/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        # link errado; mesmo de br/ipca_subitem
        # "rm/ipca_subitem_1": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/947075621",
        # "rm/ipca_subitem_2": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/847634158",
        # usar última seleção para criar
        # verificar se sao csv e se precisa de alguma config de exibição
        # todo: configurar csv na seleção e settar id_territorio
        "rm/ipca_subitem_2020": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/366270194",
        "rm/ipca_subitem_2021": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1651704499",
        "rm/ipca_subitem_2022": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1811208819",
        "rm/ipca_subitem_2023": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/1963915159",
        # https://sidra.ibge.gov.br/tabela/7063
        # "rm/inpc_subitem_1": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-884745035",
        # "rm/inpc_subitem_2": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1265010694",
        # ok
        "rm/inpc_subitem_2020": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1428442922",
        "rm/inpc_subitem_2021": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/844331411",
        "rm/inpc_subitem_2022": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1481717997",
        "rm/inpc_subitem_2023": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1485551467",
        # "rm/ip15_subitem_1": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1750716307",
        # "rm/ip15_subitem_2": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1258570016",
        # corrigir as seleções
        # https://sidra.ibge.gov.br/tabela/7062
        "rm/ip15_subitem_2020": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/51560386",
        "rm/ip15_subitem_2021": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/382082792",
        "rm/ip15_subitem_2022": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/1727207272",
        "rm/ip15_subitem_2023": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/999465717",
        "rm/ipca_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n7/all/v/all/p/all/c315/7169/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "rm/inpc_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n7/all/v/all/p/all/c315/7169/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "rm/ip15_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n7/all/v/all/p/all/c315/7169/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "mun/ipca_grupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n6/all/v/all/p/all/c315/7170,7445,7486,7558,7625,7660,7712,7766,7786/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "mun/inpc_grupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n6/all/v/all/p/all/c315/7170,7445,7486,7558,7625,7660,7712,7766,7786/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "mun/ip15_grupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n6/all/v/all/p/all/c315/7170,7445,7486,7558,7625,7660,7712,7766,7786/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "mun/ipca_subgrupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n6/all/v/all/p/all/c315/7171,7432,7446,7479,7487,7521,7548,7559,7604,7615,7620,7626,7661,7683,7697,7713,7767,7787,47656/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "mun/inpc_subgrupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n6/all/v/all/p/all/c315/7171,7432,7446,7479,7487,7521,7548,7559,7604,7615,7620,7626,7661,7683,7697,7713,7767,7787,47656/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "mun/ip15_subgrupo": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n6/all/v/all/p/all/c315/7171,7432,7446,7479,7487,7521,7548,7559,7604,7615,7620,7626,7661,7683,7697,7713,7767,7787,47656/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "mun/ipca_item": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n6/all/v/all/p/all/c315/7172,7184,7200,7219,7241,7254,7283,7303,7335,7349,7356,7372,7384,7389,7401,7415,7433,7447,7454,7461,7480,7484,7488,7495,7517,7522,7541,7549,7560,7572,7587,7605,7616,7621,7627,7640,7656,7662,7684,7690,7695,7698,7714,7730,7758,7777,7782,7788,12427,107678,109464/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "mun/inpc_item": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n6/all/v/all/p/all/c315/7172,7184,7200,7219,7241,7254,7283,7303,7335,7349,7356,7372,7384,7389,7401,7415,7433,7447,7454,7461,7480,7484,7488,7495,7517,7522,7541,7549,7560,7572,7587,7605,7616,7621,7627,7640,7656,7662,7684,7690,7695,7698,7714,7730,7758,7777,7782,7788,12427,107678,109464/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "mun/ip15_item": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n6/all/v/all/p/all/c315/7172,7184,7200,7219,7241,7254,7283,7303,7335,7349,7356,7372,7384,7389,7401,7415,7433,7447,7454,7461,7480,7484,7488,7495,7517,7522,7541,7549,7560,7572,7587,7605,7616,7621,7627,7640,7656,7662,7684,7690,7695,7698,7714,7730,7758,7777,7782,7788,12427,107678,109464/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "mun/ipca_subitem_1": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/866963382",
        "mun/ipca_subitem_2": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-113176757",
        "mun/inpc_subitem_1": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/1139761886",
        "mun/inpc_subitem_2": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1289673003",
        "mun/ip15_subitem_1": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-260564956",
        "mun/ip15_subitem_2": "https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-317614754",
        "mun/ipca_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7060.csv&terr=NC&rank=-&query=t/7060/n6/all/v/all/p/all/c315/7169/d/v63%202,v66%204,v69%202,v2265%202/l/,v,t%2Bp%2Bc315",
        "mun/inpc_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7063.csv&terr=NC&rank=-&query=t/7063/n6/all/v/all/p/all/c315/7169/d/v44%202,v45%204,v68%202,v2292%202/l/,v,t%2Bp%2Bc315",
        "mun/ip15_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela7062.csv&terr=NC&rank=-&query=t/7062/n6/all/v/all/p/all/c315/7169/d/v355%202,v356%202,v357%204,v1120%202/l/,v,t%2Bp%2Bc315",
        "mes/ipca_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela1737.csv&terr=N&rank=-&query=t/1737/n1/all/v/all/p/all/d/v63%202,v69%202,v2263%202,v2264%202,v2265%202,v2266%2013/l/,v,t%2Bp&abreviarRotulos=True&exibirNotas=False",
        "mes/ip15_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela3065.csv&terr=N&rank=-&query=t/3065/n1/all/v/all/p/all/d/v355%202,v356%202,v1117%2013,v1118%202,v1119%202,v1120%202/l/,v,t%2Bp&abreviarRotulos=True&exibirNotas=False",
        "mes/inpc_geral": "https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela1736.csv&terr=N&rank=-&query=t/1736/n1/all/v/all/p/all/d/v44%202,v68%202,v2289%2013,v2290%202,v2291%202,v2292%202/l/,v,t%2Bp&abreviarRotulos=True&exibirNotas=False",
    }

    links = {
        k: v
        for k, v in links.items()
        if k.__contains__(indice) & k.__contains__(folder)
    }
    links_keys = list(links.keys())
    log(links_keys)
    success_dwnl = []
    if folder != "rm":
        # precisei adicionar try catchs no loop para conseguir baixar todas
        # as tabelas sem ter pproblema com o limite de requisição do sidra
        for key in tqdm(links_keys):
            try:
                response = get_legacy_session().get(links[key])
                # download the csv
                with open(f"/tmp/data/input/{key}.csv", "wb") as f:
                    f.write(response.content)
                success_dwnl.append(key)
                sleep(10)
            except Exception:
                try:
                    sleep(10)
                    response = get_legacy_session().get(links[key])
                    # download the csv
                    with open(f"/tmp/data/input/{key}.csv", "wb") as f:
                        f.write(response.content)
                    success_dwnl.append(key)
                except Exception:
                    pass
    else:
        for key in tqdm(links_keys):
            try:
                response = get_legacy_session().get(links[key])
                # download the csv
                with open(f"/tmp/data/input/{key}.csv", "wb") as f:
                    f.write(response.content)
                success_dwnl.append(key)
                sleep(10)
            except Exception as e:
                log(e)
                try:
                    sleep(10)
                    response = get_legacy_session().get(links[key])
                    # download the csv
                    with open(f"/tmp/data/input/{key}.csv", "wb") as f:
                        f.write(response.content)
                    success_dwnl.append(key)
                except Exception as e:  # pylint: disable=redefined-outer-name
                    log(e)

    log(os.system("tree /tmp/data"))
    log(f"success_dwnl: {success_dwnl}")
    if len(links_keys) == len(success_dwnl):
        log("All files were successfully downloaded")
        return True
    # quebra o flow se houver erro no download de um arquivo.
    else:
        rems = set(links_keys) - set(success_dwnl)
        raise Exception(f"The following files failed to download: {rems}")


@task
def clean_mes_brasil(indice: str) -> None:
    """
    Clean the data from the mes_brasil dataset.
    """

    if indice not in ["inpc", "ipca", "ip15"]:
        raise ValueError(
            "indice argument must be one of the following: 'inpc', 'ipca', 'ip15'"
        )
    rename = {
        "Mês": "ano",
        "Geral, grupo, subgrupo, item e subitem": "categoria",
        "IPCA - Variação mensal (%)": "variacao_mensal",
        "IPCA - Variação acumulada no ano (%)": "variacao_anual",
        "IPCA - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "IPCA - Peso mensal (%)": "peso_mensal",
        "INPC - Variação mensal (%)": "variacao_mensal",
        "INPC - Variação acumulada no ano (%)": "variacao_anual",
        "INPC - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "INPC - Peso mensal (%)": "peso_mensal",
        "IPCA15 - Variação mensal (%)": "variacao_mensal",
        "IPCA15 - Variação acumulada no ano (%)": "variacao_anual",
        "IPCA15 - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "IPCA15 - Peso mensal (%)": "peso_mensal",
    }

    ordem = [
        "ano",
        "mes",
        "id_categoria",
        "id_categoria_bd",
        "categoria",
        "peso_mensal",
        "variacao_mensal",
        "variacao_anual",
        "variacao_doze_meses",
    ]

    n_mes = {
        "janeiro": "1",
        "fevereiro": "2",
        "março": "3",
        "abril": "4",
        "maio": "5",
        "junho": "6",
        "julho": "7",
        "agosto": "8",
        "setembro": "9",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12",
    }
    arquivos = [
        arquivo
        for arquivo in glob.iglob("/tmp/data/input/br/*")
        if arquivo.split("/")[-1].split("_")[0] == indice
    ]

    if len(arquivos) == 0:
        raise FileNotFoundError(
            errno.ENOENT,
            os.strerror(errno.ENOENT),
            "/tmp/data/input/br. Please, check if br is the value of FOLDER arg in crawler task and if the files was downloaded and if the files was downloaded",
        )
    for arq in arquivos:
        dataframe = pd.read_csv(arq, skipfooter=14, skiprows=2, sep=";", dtype="str")
        # renomear colunas
        dataframe.rename(columns=rename, inplace=True)
        # substituir "..." por vazio
        dataframe = dataframe.replace("...", "")
        dataframe = dataframe.replace("-", "")

        # Normalizando float
        dataframe = dataframe.replace(",", ".", regex=True)

        # Split coluna data e substituir mes
        dataframe[["mes", "ano"]] = dataframe["ano"].str.split(
            pat=" ", n=1, expand=True
        )
        dataframe["mes"] = dataframe["mes"].map(n_mes)

        # Split coluna categoria e add id_categoria_bd
        if arq.split("_")[-1].split(".")[0] != "geral":
            dataframe[["id_categoria", "categoria"]] = dataframe["categoria"].str.split(
                pat=".", n=1, expand=True
            )

        if arq.split("_")[-1].split(".")[0] == "grupo":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x + ".0.00.000"
            )
            dataframe = dataframe[ordem]
            grupo = pd.DataFrame(dataframe)
        elif arq.split("_")[-1].split(".")[0] == "subgrupo":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: f"{x[0]}.{x[1]}.00.000"
            )

            dataframe = dataframe[ordem]
            subgrupo = pd.DataFrame(dataframe)
        elif arq.split("_")[-1].split(".")[0] == "item":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: f"{x[0]}.{x[1]}.{x[2:4]}.000"
            )

            dataframe = dataframe[ordem]
            item = pd.DataFrame(dataframe)
        elif arq.split("_")[-1].split(".")[0] == "subitem":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: f"{x[0]}.{x[1]}.{x[2:4]}.{x[4:7]}"
            )

            dataframe = dataframe[ordem]
            subitem = pd.DataFrame(dataframe)
        elif arq.split("_")[-1].split(".")[0] == "geral":
            dataframe["id_categoria"] = ""
            dataframe["id_categoria_bd"] = "0.0.00.000"
            dataframe = dataframe[ordem]
            geral = pd.DataFrame(dataframe)

    # pylint: disable=E0602
    # Add only dataframes defined in previous loop. Download failure leads to some dataframe not being defined
    files_dict = {
        "grupo": grupo if "grupo" in locals() else "",
        "subgrupo": subgrupo if "subgrupo" in locals() else "",
        "item": item if "item" in locals() else "",
        "subitem": subitem if "subitem" in locals() else "",
        "geral": geral if "geral" in locals() else "",
    }

    downloaded = [
        k for k in files_dict.keys() if isinstance(files_dict[k], pd.DataFrame)
    ]
    dataframe = pd.concat([files_dict[k] for k in downloaded])
    filepath = f"/tmp/data/output/{indice}/categoria_brasil.csv"
    dataframe.to_csv(filepath, index=False)
    log(os.system("tree /tmp/data"))

    return filepath


@task
def clean_mes_rm(indice: str):
    """
    Clean mes_rm
    """
    if indice not in ["inpc", "ipca", "ip15"]:
        raise ValueError(
            "indice argument must be one of the following: 'inpc', 'ipca', 'ip15'"
        )
    rename = {
        "Cód.": "id_regiao_metropolitana",
        "Unnamed: 1": "rm",
        "Mês": "ano",
        "Geral, grupo, subgrupo, item e subitem": "categoria",
        "IPCA - Variação mensal (%)": "variacao_mensal",
        "IPCA - Variação acumulada no ano (%)": "variacao_anual",
        "IPCA - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "IPCA - Peso mensal (%)": "peso_mensal",
        "INPC - Variação mensal (%)": "variacao_mensal",
        "INPC - Variação acumulada no ano (%)": "variacao_anual",
        "INPC - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "INPC - Peso mensal (%)": "peso_mensal",
        "IPCA15 - Variação mensal (%)": "variacao_mensal",
        "IPCA15 - Variação acumulada no ano (%)": "variacao_anual",
        "IPCA15 - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "IPCA15 - Peso mensal (%)": "peso_mensal",
    }

    ordem = [
        "ano",
        "mes",
        "id_regiao_metropolitana",
        "id_categoria",
        "id_categoria_bd",
        "categoria",
        "peso_mensal",
        "variacao_mensal",
        "variacao_anual",
        "variacao_doze_meses",
    ]

    n_mes = {
        "janeiro": "1",
        "fevereiro": "2",
        "março": "3",
        "abril": "4",
        "maio": "5",
        "junho": "6",
        "julho": "7",
        "agosto": "8",
        "setembro": "9",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12",
    }
    arquivos = [
        arquivo
        for arquivo in glob.iglob("/tmp/data/input/rm/*")
        if arquivo.split("/")[-1].split("_")[0] == indice
    ]

    if len(arquivos) == 0:
        raise FileNotFoundError(
            errno.ENOENT,
            os.strerror(errno.ENOENT),
            "/tmp/data/input/rm. Please, check if rm is the value of FOLDER arg in crawler task and if the files was downloaded",
        )

    for arq in arquivos:
        log(arq)
        try:
            dataframe = pd.read_csv(
                arq, skipfooter=14, skiprows=2, sep=";", dtype="str"
            )
        except Exception as e:
            log(
                f"Error reading {arq}: {e}. Check the the file. It may have surparsed the 200.000 values download limit of IBGE SIDRA API"
            )
            break
        # renomear colunas.
        dataframe.rename(columns=rename, inplace=True)
        # substituir "..." por vazio
        dataframe = dataframe.replace("...", "")
        dataframe = dataframe.replace("-", "")

        # Normalizando float
        dataframe = dataframe.replace(",", ".", regex=True)

        # Split coluna data e substituir mes
        dataframe[["mes", "ano"]] = dataframe["ano"].str.split(
            pat=" ", n=1, expand=True
        )
        dataframe["mes"] = dataframe["mes"].map(n_mes)

        # Split coluna categoria e add id_categoria_bd
        if arq.split("_")[-1].split(".")[0] != "geral":
            dataframe[["id_categoria", "categoria"]] = dataframe["categoria"].str.split(
                pat=".", n=1, expand=True
            )

        if arq.split("_")[-1].split(".")[0] == "grupo":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x + ".0.00.000"
            )
            dataframe = dataframe[ordem]
            grupo = pd.DataFrame(dataframe)
        elif arq.split("_")[-1].split(".")[0] == "subgrupo":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x[0] + "." + x[1] + ".00.000"
            )
            dataframe = dataframe[ordem]
            subgrupo = pd.DataFrame(dataframe)
        elif arq.split("_")[-1].split(".")[0] == "item":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x[0] + "." + x[1] + "." + x[2:4] + ".000"
            )
            dataframe = dataframe[ordem]
            item = pd.DataFrame(dataframe)

        elif "_".join(arq.split("_")[1:]).split(".", maxsplit=1)[0] == "subitem_2020":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x[0] + "." + x[1] + "." + x[2:4] + "." + x[4:7]
            )
            dataframe = dataframe[ordem]
            subitem_2020 = pd.DataFrame(dataframe)

        elif "_".join(arq.split("_")[1:]).split(".", maxsplit=1)[0] == "subitem_2021":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x[0] + "." + x[1] + "." + x[2:4] + "." + x[4:7]
            )
            dataframe = dataframe[ordem]
            subitem_2021 = pd.DataFrame(dataframe)

        elif "_".join(arq.split("_")[1:]).split(".", maxsplit=1)[0] == "subitem_2022":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x[0] + "." + x[1] + "." + x[2:4] + "." + x[4:7]
            )
            dataframe = dataframe[ordem]
            subitem_2022 = pd.DataFrame(dataframe)

        elif "_".join(arq.split("_")[1:]).split(".", maxsplit=1)[0] == "subitem_2023":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x[0] + "." + x[1] + "." + x[2:4] + "." + x[4:7]
            )
            dataframe = dataframe[ordem]
            subitem_2023 = pd.DataFrame(dataframe)

        # elif "_".join(arq.split("_")[1:]).split(".", maxsplit=1)[0] == "subitem_1":
        #    dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
        #        lambda x: x[0] + "." + x[1] + "." + x[2:4] + "." + x[4:7]
        #    )
        #    dataframe = dataframe[ordem]
        #    subitem_1 = pd.DataFrame(dataframe)
        # todo: criar mais 3 elifs
        # todo: com mesmo padrao de nome dos links

        # elif "_".join(arq.split("_")[1:]).split(".", maxsplit=1)[0] == "subitem_2":
        #    dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
        #        lambda x: x[0] + "." + x[1] + "." + x[2:4] + "." + x[4:7]
        #    )
        #    dataframe = dataframe[ordem]
        #    subitem_2 = pd.DataFrame(dataframe)

        elif arq.split("_")[-1].split(".")[0] == "geral":
            dataframe["id_categoria"] = ""
            dataframe["id_categoria_bd"] = "0.0.00.000"
            dataframe = dataframe[ordem]
            geral = pd.DataFrame(dataframe)

    # Add only dataframes defined in previous loop. Download failure leads to some dataframe not being defined
    files_dict = {
        # todo: adicionar os demais subitesm
        "grupo": grupo if "grupo" in locals() else "",
        "subgrupo": subgrupo if "subgrupo" in locals() else "",
        "item": item if "item" in locals() else "",
        "subitem_2020": subitem_2020 if "subitem_2020" in locals() else "",
        "subitem_2021": subitem_2021 if "subitem_2021" in locals() else "",
        "subitem_2022": subitem_2022 if "subitem_2022" in locals() else "",
        "subitem_2023": subitem_2023 if "subitem_2023" in locals() else "",
        # "subitem_1": subitem_1 if "subitem_1" in locals() else "",
        # "subitem_2": subitem_2 if "subitem_2" in locals() else "",
        "geral": geral if "geral" in locals() else "",
    }

    downloaded = [
        k for k in files_dict.keys() if isinstance(files_dict[k], pd.DataFrame)
    ]
    dataframe = pd.concat([files_dict[k] for k in downloaded])
    filepath = f"/tmp/data/output/{indice}/categoria_rm.csv"
    dataframe.to_csv(filepath, index=False)

    return filepath


@task
def clean_mes_municipio(indice: str):
    """
    Clean mes_municipio
    """
    if indice not in ["inpc", "ipca", "ip15"]:
        raise ValueError(
            "indice argument must be one of the following: 'inpc', 'ipca', 'ip15'"
        )
    rename = {
        "Cód.": "id_municipio",
        "Unnamed: 1": "municipio",
        "Mês": "ano",
        "Geral, grupo, subgrupo, item e subitem": "categoria",
        "IPCA - Variação mensal (%)": "variacao_mensal",
        "IPCA - Variação acumulada no ano (%)": "variacao_anual",
        "IPCA - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "IPCA - Peso mensal (%)": "peso_mensal",
        "INPC - Variação mensal (%)": "variacao_mensal",
        "INPC - Variação acumulada no ano (%)": "variacao_anual",
        "INPC - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "INPC - Peso mensal (%)": "peso_mensal",
        "IPCA15 - Variação mensal (%)": "variacao_mensal",
        "IPCA15 - Variação acumulada no ano (%)": "variacao_anual",
        "IPCA15 - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "IPCA15 - Peso mensal (%)": "peso_mensal",
    }

    ordem = [
        "ano",
        "mes",
        "id_municipio",
        "id_categoria",
        "id_categoria_bd",
        "categoria",
        "peso_mensal",
        "variacao_mensal",
        "variacao_anual",
        "variacao_doze_meses",
    ]

    n_mes = {
        "janeiro": "1",
        "fevereiro": "2",
        "março": "3",
        "abril": "4",
        "maio": "5",
        "junho": "6",
        "julho": "7",
        "agosto": "8",
        "setembro": "9",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12",
    }
    arquivos = [
        arquivo
        for arquivo in glob.iglob("/tmp/data/input/mun/*")
        if arquivo.split("/")[-1].split("_")[0] == indice
    ]

    if len(arquivos) == 0:
        raise FileNotFoundError(
            errno.ENOENT,
            os.strerror(errno.ENOENT),
            "/tmp/data/input/mun. Please, check if mun is the value of FOLDER arg in crawler task and if the files was downloaded",
        )

    for arq in arquivos:
        dataframe = pd.read_csv(arq, skipfooter=14, skiprows=2, sep=";", dtype="str")
        # renomear colunas
        dataframe.rename(columns=rename, inplace=True)
        # substituir "..." por vazio
        dataframe = dataframe.replace("...", "")
        dataframe = dataframe.replace("-", "")

        # Normalizando float
        dataframe = dataframe.replace(",", ".", regex=True)

        # Split coluna data e substituir mes
        dataframe[["mes", "ano"]] = dataframe["ano"].str.split(
            pat=" ", n=1, expand=True
        )
        dataframe["mes"] = dataframe["mes"].map(n_mes)

        # Split coluna categoria e add id_categoria_bd
        if arq.split("_")[-1].split(".")[0] != "geral":
            dataframe[["id_categoria", "categoria"]] = dataframe["categoria"].str.split(
                pat=".", n=1, expand=True
            )

        if arq.split("_")[-1].split(".")[0] == "grupo":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x + ".0.00.000"
            )
            dataframe = dataframe[ordem]
            grupo = pd.DataFrame(dataframe)
        elif arq.split("_")[-1].split(".")[0] == "subgrupo":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x[0] + "." + x[1] + ".00.000"
            )
            dataframe = dataframe[ordem]
            subgrupo = pd.DataFrame(dataframe)
        elif arq.split("_")[-1].split(".")[0] == "item":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x[0] + "." + x[1] + "." + x[2:4] + ".000"
            )
            dataframe = dataframe[ordem]
            item = pd.DataFrame(dataframe)
        elif "_".join(arq.split("_")[1:]).split(".", maxsplit=1)[0] == "subitem_1":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x[0] + "." + x[1] + "." + x[2:4] + "." + x[4:7]
            )
            dataframe = dataframe[ordem]
            subitem_1 = pd.DataFrame(dataframe)
        elif "_".join(arq.split("_")[1:]).split(".", maxsplit=1)[0] == "subitem_2":
            dataframe["id_categoria_bd"] = dataframe["id_categoria"].apply(
                lambda x: x[0] + "." + x[1] + "." + x[2:4] + "." + x[4:7]
            )
            dataframe = dataframe[ordem]
            subitem_2 = pd.DataFrame(dataframe)
        elif arq.split("_")[-1].split(".")[0] == "geral":
            dataframe["id_categoria"] = ""
            dataframe["id_categoria_bd"] = "0.0.00.000"
            dataframe = dataframe[ordem]
            geral = pd.DataFrame(dataframe)

    # Add only dataframes defined in previous loop. Download failure leads to some dataframe not being defined
    files_dict = {
        "grupo": grupo if "grupo" in locals() else "",
        "subgrupo": subgrupo if "subgrupo" in locals() else "",
        "item": item if "item" in locals() else "",
        "subitem_1": subitem_1 if "subitem_1" in locals() else "",
        "subitem_2": subitem_2 if "subitem_2" in locals() else "",
        "geral": geral if "geral" in locals() else "",
    }

    downloaded = [
        k for k in files_dict.keys() if isinstance(files_dict[k], pd.DataFrame)
    ]
    dataframe = pd.concat([files_dict[k] for k in downloaded])
    filepath = f"/tmp/data/output/{indice}/categoria_municipio.csv"
    dataframe.to_csv(filepath, index=False)

    return filepath


@task
def clean_mes_geral(indice: str):
    """
    clean_mes_geral
    """
    if indice not in ["inpc", "ipca", "ip15"]:
        raise ValueError(
            "indice argument must be one of the following: 'inpc', 'ipca', 'ip15'"
        )
    rename = {
        "IPCA - Número-índice (base: dezembro de 1993 = 100) (Número-índice)": "indice",
        "IPCA - Variação mensal (%)": "variacao_mensal",
        "IPCA - Variação acumulada em 3 meses (%)": "variacao_trimestral",
        "IPCA - Variação acumulada em 6 meses (%)": "variacao_semestral",
        "IPCA - Variação acumulada no ano (%)": "variacao_anual",
        "IPCA - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "IPCA15 - Número-índice (base: dezembro de 1993 = 100) (Número-índice)": "indice",
        "IPCA15 - Variação mensal (%)": "variacao_mensal",
        "IPCA15 - Variação acumulada em 3 meses (%)": "variacao_trimestral",
        "IPCA15 - Variação acumulada em 6 meses (%)": "variacao_semestral",
        "IPCA15 - Variação acumulada no ano (%)": "variacao_anual",
        "IPCA15 - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
        "INPC - Número-índice (base: dezembro de 1993 = 100) (Número-índice)": "indice",
        "INPC - Variação mensal (%)": "variacao_mensal",
        "INPC - Variação acumulada em 3 meses (%)": "variacao_trimestral",
        "INPC - Variação acumulada em 6 meses (%)": "variacao_semestral",
        "INPC - Variação acumulada no ano (%)": "variacao_anual",
        "INPC - Variação acumulada em 12 meses (%)": "variacao_doze_meses",
    }

    ordem = [
        "ano",
        "mes",
        "indice",
        "variacao_mensal",
        "variacao_trimestral",
        "variacao_semestral",
        "variacao_anual",
        "variacao_doze_meses",
    ]

    n_mes = {
        "janeiro": "1",
        "fevereiro": "2",
        "março": "3",
        "abril": "4",
        "maio": "5",
        "junho": "6",
        "julho": "7",
        "agosto": "8",
        "setembro": "9",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12",
    }
    arquivos = [
        arquivo
        for arquivo in glob.iglob("/tmp/data/input/mes/*")
        if arquivo.split("/")[-1].split("_")[0] == indice
    ]

    if len(arquivos) == 0:
        raise FileNotFoundError(
            errno.ENOENT,
            os.strerror(errno.ENOENT),
            "/tmp/data/input/mes. Please, check if mes is the value of FOLDER arg in crawler task and if the files was downloaded",
        )

    for arq in arquivos:
        log(arq)
        if indice == "ip15":
            dataframe = pd.read_csv(arq, skiprows=2, skipfooter=11, sep=";")
        else:
            dataframe = pd.read_csv(arq, skiprows=2, skipfooter=13, sep=";")
        dataframe[["mes", "ano"]] = dataframe["Mês"].str.split(" ", n=1, expand=True)
        dataframe["mes"] = dataframe["mes"].map(n_mes)

        # renomear colunas
        dataframe.rename(columns=rename, inplace=True)
        dataframe = dataframe.replace("...", "")
        dataframe = dataframe.replace("-", "")

        # Normalizando float
        dataframe = dataframe.replace(",", ".", regex=True)

        # Renomeando colunas e ordenando
        dataframe = dataframe[ordem]

    filepath = f"/tmp/data/output/{indice}/mes_brasil.csv"
    dataframe.to_csv(filepath, index=False)
    log(os.system("tree /tmp/data"))

    return filepath
