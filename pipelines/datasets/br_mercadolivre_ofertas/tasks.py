# -*- coding: utf-8 -*-
""" Tasks for Mercado Livre Ofertas dataset """
# pylint: disable=invalid-name


import asyncio
import time
import os
from typing import List, Tuple

from prefect import task
import pandas as pd

from pipelines.utils.tasks import log
from pipelines.datasets.br_mercadolivre_ofertas.constants import (
    constants as const_mercadolivre,
)
from pipelines.datasets.br_mercadolivre_ofertas.utils import (
    main_item,
    main_seller,
    get_id,
    clean_experience,
)

new_cols_item = const_mercadolivre.NEW_ORDER_COLS.value
new_order_clean = const_mercadolivre.NEW_ORDER_CLEAN.value
new_order_item = const_mercadolivre.NEW_ORDER_ITEM.value
oferta_dia = const_mercadolivre.OFERTA_DIA.value
kwargs_list = const_mercadolivre.KWARGS_LIST.value
url_lists = {"oferta_dia": []}

for i in range(1, 21):
    urls = {"oferta_dia": oferta_dia + str(i)}
    for table, url in urls.items():
        url_lists[table].append(url)


@task
def crawler_mercadolivre_item():
    """
    Executes the crawler for Mercado Livre offers by running the main process, processing the results,
    and saving them to a CSV file.

    Returns:
        str: The path to the CSV file containing the collected item data.
    """
    loop = asyncio.get_event_loop()
    # urls_item = url_lists['less100']
    contents = loop.run_until_complete(main_item(url_lists, kwargs_list))
    time.sleep(5)
    df = pd.DataFrame(contents)
    total = df.shape[0]
    df = df.dropna(subset=["title"])
    remained = df.shape[0]
    # print percentage keeped
    log(f"Percentage keeped: {remained/total*100:.2f}%")
    df = df[new_order_item]
    df = df.astype(str)
    filepath = "/tmp/items_raw.csv"
    df.to_csv(filepath, index=False)

    loop.close()

    return filepath


@task
def clean_item(filepath):
    """
    Cleans and preprocesses item data read from a CSV file. The cleaned data includes columns renaming,
    reordering, duplicate removal, text cleaning, type conversion, and saving to a specific directory.

    Args:
        filepath (str): The path to the CSV file containing the raw item data.

    Returns:
        str: The path to the directory where the cleaned item data is saved.
    """
    item = pd.read_csv(filepath)
    # rename columns
    item.columns = new_cols_item
    # change order
    item = item.reindex(new_order_clean, axis=1)

    # drop dupliacte item_id
    item = item.drop_duplicates(subset=["item_id"])

    # clean quantidade_avaliacoes: (11004)->11004
    # item["quantidade_avaliacoes"] = item["quantidade_avaliacoes"].str.replace("(", "")
    # item["quantidade_avaliacoes"] = item["quantidade_avaliacoes"].str.replace(")", "")

    # clean desconto: 10% OFF -> 10
    item["desconto"] = item["desconto"].str.replace("% OFF", "")
    # remove if title is nan
    item = item[item["titulo"].notna()]
    # remove item_link
    item = item.drop("url_item", axis=1)
    # remove link_vendedor
    item = item.drop("link_vendedor", axis=1)
    # make envio_pais a boolean
    item["envio_pais"] = item["envio_pais"].astype(str)
    item["envio_pais"] = item["envio_pais"].str.contains("Envio para todo o país")
    # make nan equal to False
    item["envio_pais"] = item["envio_pais"].fillna(False)

    # to string
    # item = item.astype(str)

    today = pd.Timestamp.today().strftime("%Y-%m-%d")

    os.system(f"mkdir -p /tmp/data/br_mercadolivre_ofertas/item/dia={today}")

    item.to_csv(
        f"/tmp/data/br_mercadolivre_ofertas/item/dia={today}/items.csv", index=False
    )

    return "/tmp/data/br_mercadolivre_ofertas/item/"


@task
def crawler_mercadolivre_seller(seller_ids, seller_links):
    """
    Crawl and collect seller data from Mercado Livre using provided seller IDs and links.

    Args:
        seller_ids (list): A list of seller IDs to collect data for.
        seller_links (list): A list of seller links to crawl data from.

    Returns:
        str: The path to the CSV file containing the collected seller data.
    """
    filepath_raw = "vendedor.csv"
    asyncio.run(main_seller(seller_ids, seller_links, filepath_raw))

    return filepath_raw


@task
def clean_seller(filepath_raw):
    """
    Clean and process raw seller data collected from Mercado Livre.

    This function takes the path to a CSV file containing raw seller data collected from Mercado Livre.
    It cleans and processes the data, performing operations such as renaming columns, cleaning text,
    converting formats, and reordering columns. The cleaned data is saved to a new CSV file.

    Args:
        filepath_raw (str): The path to the raw seller data CSV file.

    Returns:
        str: The path to the directory containing the cleaned seller data CSV file.
    """

    seller = pd.read_csv(filepath_raw)
    log(seller.head(5))

    new_cols = [
        "nome",
        "experiencia",
        "reputacao",
        "classificacao",
        "localizacao",
        "opinioes",
        "data",
        "vendedor_id",
    ]

    seller.columns = new_cols
    # remove if title is nan
    seller = seller[seller["nome"].notna()]
    # clean experiencia: 3 anos vendendo no Mercado Livre -> 3
    seller["experiencia"] = seller["experiencia"].apply(clean_experience)
    # clean classificacao: MercadoLíder Platinum -> Platinum
    seller["classificacao"] = seller["classificacao"].str.replace("MercadoLíder ", "")
    # clean localizacao: LocalizaçãoJuiz de Fora, Minas Gerais. -> Juiz de Fora, Minas Gerais.
    seller["localizacao"] = seller["localizacao"].str.replace("Localização", "")
    # clean opinioes: [{'Bom': 771, 'Regular': 67, 'Ruim': 174}] -> {'Bom': 771, 'Regular': 67, 'Ruim': 174}
    dict_municipios = const_mercadolivre.MAP_MUNICIPIO_TO_ID.value
    seller["localizacao"] = seller["localizacao"].apply(
        lambda x: get_id(x, dict_municipios)
    )
    # rename localizacao to id_municipio
    seller = seller.rename(columns={"localizacao": "id_municipio"})
    seller["opinioes"] = seller["opinioes"].str.replace("[", "")
    seller["opinioes"] = seller["opinioes"].str.replace("]", "")

    # put vendedor_id in the first column
    new_order = [
        "vendedor_id",
        "nome",
        "experiencia",
        "reputacao",
        "classificacao",
        "id_municipio",
        "opinioes",
        "data",
    ]
    seller = seller.reindex(new_order, axis=1)
    # drop where experiencia is nan
    seller = seller[seller["experiencia"].notna()]
    # drop data column
    seller = seller.drop("data", axis=1)

    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    os.system(f"mkdir -p /tmp/data/br_mercadolivre_ofertas/vendedor/dia={today}")
    seller.to_csv(
        f"/tmp/data/br_mercadolivre_ofertas/vendedor/dia={today}/seller.csv",
        index=False,
    )

    return "/tmp/data/br_mercadolivre_ofertas/vendedor/"


@task(nout=2)
def get_today_sellers(filepath_raw) -> Tuple[List[str], List[str]]:
    """
    Extracts unique seller IDs and their corresponding links from cleaned seller data.
    The extracted information is returned as two lists: one containing unique seller IDs
    and the other containing their corresponding links.

    Args:
        filepath_raw (str): The path to the cleaned seller data CSV file.

    Returns:
        Tuple[List[str], List[str]]: A tuple of two lists, where the first list contains unique seller IDs
        and the second list contains their corresponding links.
    """
    df = pd.read_csv(filepath_raw)

    # remove nan in seller_link column
    df = df[df["seller_link"].notna()]
    df = df[df["seller_id"].notna()]

    # remove duplicate sellers
    df = df.drop_duplicates(subset=["seller_id"])
    log(f"Number of sellers: {len(df)}")

    if df.empty:
        return [], []
    # get list of unique sellers
    dict_id_link = dict(zip(df["seller_id"], df["seller_link"]))

    return list(dict_id_link.keys()), list(dict_id_link.values())


@task
def is_empty_list(list_sellers: List[str]) -> bool:
    """
    Checks if a list of sellers is empty.

    Args:
        list_sellers (List[str]): A list of seller IDs.

    Returns:
        bool: Returns `True` if the input list is empty, otherwise returns `False`.
    """
    return len(list_sellers) == 0
