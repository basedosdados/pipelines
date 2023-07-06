# -*- coding: utf-8 -*-
""" Tasks for Mercado Livre Ofertas dataset """
# pylint: disable=invalid-name


import asyncio
import time
import os
import re
from typing import List, Tuple

from prefect import task
import pandas as pd

from pipelines.utils.tasks import log
from pipelines.datasets.mercadolivre_ofertas.constants import (
    constants as const_mercadolivre,
)
from pipelines.datasets.mercadolivre_ofertas.utils import main_item, main_seller, get_id

less100 = const_mercadolivre.LESS100.value
oferta_dia = const_mercadolivre.OFERTA_DIA.value
relampago = const_mercadolivre.RELAMPAGO.value
barato_dia = const_mercadolivre.BARATO_DIA.value
kwargs_list = const_mercadolivre.KWARGS_LIST.value
tables_names = const_mercadolivre.TABLES_NAMES.value

urls = [less100, oferta_dia, relampago, barato_dia]
dict_tables = dict(zip(tables_names, urls))


@task
def crawler_mercadolivre_item():
    """
    Executes the crawler for Mercado Livre offers by running the main process, processing the results,
    and saving them to a CSV file.

    Returns:
        str: The file path of the generated CSV file.

    Raises:
        None
    """
    loop = asyncio.get_event_loop()
    contents = loop.run_until_complete(main_item(dict_tables, kwargs_list))
    time.sleep(5)
    df = pd.DataFrame(contents)
    total = df.shape[0]
    df = df.dropna(subset=["title"])
    remained = df.shape[0]
    # print percentage keeped
    print(f"Percentage keeped: {remained/total*100:.2f}%")
    new_order = [
        "title",
        "review_amount",
        "discount",
        "transport_condition",
        "stars",
        "price",
        "price_original",
        "item_id_bd",
        "seller_link",
        "seller_id",
        "seller",
        "datetime",
        "site_section",
        "features",
        "item_url",
        "categories",
    ]
    df = df[new_order]
    df = df.astype(str)
    filepath = "/tmp/items_raw.csv"
    df.to_csv(filepath, index=False)

    log(df.head(5))

    loop.close()

    return filepath


@task
def clean_item(filepath):
    item = pd.read_csv(filepath)
    new_cols = [
        "titulo",
        "quantidade_avaliacoes",
        "desconto",
        "envio_pais",
        "estrelas",
        "preco",
        "preco_original",
        "item_id",
        "link_vendedor",
        "id_vendedor",
        "vendedor",
        "data_hora",
        "secao_site",
        "caracteristicas",
        "url_item",
        "categorias",
    ]
    # rename columns
    item.columns = new_cols
    # change order
    new_order = [
        "data_hora",
        "titulo",
        "item_id",
        "categorias",
        "quantidade_avaliacoes",
        "desconto",
        "envio_pais",
        "estrelas",
        "preco",
        "preco_original",
        "id_vendedor",
        "vendedor",
        "link_vendedor",
        "secao_site",
        "caracteristicas",
        "url_item",
    ]
    item = item.reindex(new_order, axis=1)
    # drop dupliacte item_id
    item = item.drop_duplicates(subset=["item_id"])
    # clean quantidade_avaliacoes: (11004)->11004
    item["quantidade_avaliacoes"] = item["quantidade_avaliacoes"].str.replace("(", "")
    item["quantidade_avaliacoes"] = item["quantidade_avaliacoes"].str.replace(")", "")
    # clean desconto: 10% OFF -> 10
    item["desconto"] = item["desconto"].str.replace("% OFF", "")
    # clean categorias. Currently, it's a list of lists. Transform into a list of strings. First it's necessary to transform the string into a list of lists
    # item["categorias"] = item["categorias"].str.replace("[[", "[")
    # item["categorias"] = item["categorias"].str.replace("]]", "]")
    # remove if title is nan
    item = item[item["titulo"].notna()]
    # remove item_link
    item = item.drop("url_item", axis=1)
    # remove link_vendedor
    item = item.drop("link_vendedor", axis=1)
    # make envio_pais a boolean
    item["envio_pais"] = item["envio_pais"].str.contains("Envio para todo o país")
    # make nan equal to False
    item["envio_pais"] = item["envio_pais"].fillna(False)

    # to string
    item = item.astype(str)

    today = pd.Timestamp.today().strftime("%Y-%m-%d")

    os.system(f"mkdir -p br_mercadolivre_ofertas/item/dia={today}")

    item.to_csv(f"br_mercadolivre_ofertas/item/dia={today}/items.csv", index=False)

    return "br_mercadolivre_ofertas/item/"


@task
def crawler_mercadolivre_seller(seller_ids, seller_links):
    filepath_raw = "vendedor.csv"
    log('Type of seller_ids: ', type(seller_ids))
    log('Type of seller_links: ', type(seller_links))
    asyncio.run(main_seller(seller_ids, seller_links, filepath_raw))

    return filepath_raw


@task
def clean_seller(filepath_raw):
    seller = pd.read_csv(filepath_raw)

    new_cols = [
        "nome",
        "experiencia",
        "reputacao",
        "classificacao",
        "localizacao",
        "opinioes",
        "data",
        "id_vendor",
    ]

    seller.columns = new_cols
    # remove if title is nan
    seller = seller[seller["nome"].notna()]
    # clean experiencia: 3 anos vendendo no Mercado Livre -> 3
    seller["experiencia"] = seller["experiencia"].apply(
        lambda x: re.findall(r"\d+", x)[0]
    )
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

    # put id_vendor in the first column
    new_order = [
        "id_vendor",
        "nome",
        "experiencia",
        "reputacao",
        "classificacao",
        "id_municipio",
        "opinioes",
        "data",
    ]
    seller = seller.reindex(new_order, axis=1)

    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    os.system(f"mkdir -p br_mercadolivre_ofertas/vendedor/dia={today}")
    seller.to_csv(
        f"br_mercadolivre_ofertas/vendedor/dia={today}/seller.csv", index=False
    )

    return "br_mercadolivre_ofertas/vendedor/"


@task(nout=2)
def get_today_sellers(filepath_raw) -> Tuple[List[str], List[str]]:
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
@task
def is_empty_list(list_sellers: List[str]) -> bool:
    return len(list_sellers) == 0
