# -*- coding: utf-8 -*-
""" Tasks for Mercado Livre Ofertas dataset """
# pylint: disable=invalid-name


import asyncio
import time
from prefect import task

import pandas as pd

from pipelines.utils.tasks import log
from pipelines.datasets.mercadolivre_ofertas.constants import (
    constants as const_mercadolivre,
)
from pipelines.datasets.mercadolivre_ofertas.utils import main

less100 = const_mercadolivre.LESS100.value
oferta_dia = const_mercadolivre.OFERTA_DIA.value
relampago = const_mercadolivre.RELAMPAGO.value
barato_dia = const_mercadolivre.BARATO_DIA.value
kwargs_list = const_mercadolivre.KWARGS_LIST.value
tables_names = const_mercadolivre.TABLES_NAMES.value

urls = [less100, oferta_dia, relampago, barato_dia]
dict_tables = dict(zip(tables_names, urls))


@task
def crawler_mercadolivre_ofertas():
    """
    Executes the crawler for Mercado Livre offers by running the main process, processing the results,
    and saving them to a CSV file.

    Returns:
        str: The file path of the generated CSV file.

    Raises:
        None
    """
    loop = asyncio.get_event_loop()
    contents = loop.run_until_complete(main(dict_tables, kwargs_list))
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
