# -*- coding: utf-8 -*-
"""
Tasks for botdosdados
"""
import os
from typing import Tuple
from datetime import timedelta, datetime
from collections import defaultdict

import tweepy
from prefect import task
from basedosdados.download.metadata import _safe_fetch
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pipelines.utils.utils import log, get_storage_blobs
from pipelines.datasets.botdosdados.utils import (
    get_credentials_from_secret,
)
from pipelines.constants import constants


# pylint: disable=C0103
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def echo(message: str) -> None:
    """
    Logs message as a Task
    """
    log(message)


@task(checkpoint=False, nout=5)
def get_credentials(secret_path: str) -> Tuple[str, str, str, str, str]:
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    tokens_dict = get_credentials_from_secret(secret_path)
    access_token_secret = tokens_dict["ACCESS_SECRET"]
    access_token = tokens_dict["ACCESS_TOKEN"]
    consumer_key = tokens_dict["CONSUMER_KEY"]
    consumer_secret = tokens_dict["CONSUMER_SECRET"]
    twitter_token = tokens_dict["TWITTER_TOKEN"]

    return (
        access_token_secret,
        access_token,
        consumer_key,
        consumer_secret,
        twitter_token,
    )


# pylint: disable=R0914
# pylint: disable=R0913
# pylint: disable=W0613
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def was_table_updated(page_size: int, hours: int, subset: str, wait=None) -> bool:
    """
    Checks if there are tables updated within last hour. If True, saves table locally.
    """

    # pylint: disable=R0915
    datasets_links = defaultdict(lambda: "not selected")
    datasets_links[
        "mundo_transfermarkt_competicoes"
    ] = "https://basedosdados.org/dataset/mundo-transfermarkt-competicoes"
    datasets_links[
        "br_me_caged.microdados_antigos"
    ] = "https://basedosdados.org/dataset/br-me-caged"
    datasets_links["br_ibge_inpc"] = "https://basedosdados.org/dataset/br-ibge-inpc"
    datasets_links["br_ibge_ipca"] = "https://basedosdados.org/dataset/br-ibge-ipca"
    datasets_links["br_ibge_ipca15"] = "https://basedosdados.org/dataset/br-ibge-ipca15"
    datasets_links[
        "br_anp_precos_combustiveis"
    ] = "https://basedosdados.org/dataset/br-anp-precos-combustiveis"
    datasets_links[
        "br_poder360_pesquisas"
    ] = "https://basedosdados.org/dataset/br-poder360-pesquisas"
    datasets_links["br_ms_cnes"] = "https://basedosdados.org/dataset/br-ms-cnes"
    datasets_links[
        "br_camara_atividade_legislativa"
    ] = "https://basedosdados.org/dataset/br-camara-atividade-legislativa"
    datasets_links["br_ibge_pnadc"] = "https://basedosdados.org/dataset/br-ibge-pnadc"
    datasets_links[
        "br_ana_reservatorios"
    ] = "https://basedosdados.org/dataset/br-ana-reservatorios"

    if subset == "inflation":
        datasets_links = {
            k: v
            for k, v in datasets_links.items()
            if k in ["br_ibge_inpc", "br_ibge_ipca", "br_ibge_ipca15"]
        }
    else:
        raise ValueError("Subset must me one of the following: inflation")

    selected_datasets = list(datasets_links.keys())

    url = f"https://basedosdados.org/api/3/action/bd_dataset_search?q=&resource_type=bdm_table&page=1&page_size={page_size}"
    response = _safe_fetch(url)
    json_response = response.json()
    datasets = json_response["result"]["datasets"]
    n_datasets = len(datasets)
    dfs = []
    for index in range(n_datasets):
        dataset_dict = datasets[index]
        for j in range(len(dataset_dict["resources"])):
            if dataset_dict["resources"][j]["resource_type"] == "bdm_table":
                dataset_name = dataset_dict["resources"][j]["dataset_id"]
                break
            continue
        n_tables = len(dataset_dict["resources"])
        dataset_resources = [
            dataset_dict["resources"][k]
            for k in range(n_tables)
            if "last_updated" in dataset_dict["resources"][k].keys()
        ]
        dataset_resources = [
            dataset_resource
            for dataset_resource in dataset_resources
            if dataset_resource["last_updated"] is not None
        ]
        dataset_resources = [
            dataset_resource
            for dataset_resource in dataset_resources
            if dataset_resource["resource_type"] == "bdm_table"
        ]
        n_tables = len(dataset_resources)
        tables = [dataset_resources[k]["name"] for k in range(n_tables)]
        last_updated = [
            dataset_resources[k]["last_updated"]["data"] for k in range(n_tables)
        ]
        temporal_coverage = [
            dataset_resources[k]["temporal_coverage"] for k in range(n_tables)
        ]
        updated_frequency = [
            dataset_resources[k]["update_frequency"] for k in range(n_tables)
        ]
        df = pd.DataFrame(
            {
                "table": tables,
                "last_updated": last_updated,
                "temporal_coverage": temporal_coverage,
                "updated_frequency": updated_frequency,
            }
        )
        df["dataset"] = dataset_name
        df = df.reindex(
            [
                "dataset",
                "table",
                "last_updated",
                "temporal_coverage",
                "updated_frequency",
            ],
            axis=1,
        )
        dfs.append(df)

    df = dfs[0].append(dfs[1:])
    df["link"] = df["dataset"].map(datasets_links)
    df["last_updated"] = [
        datetime.strptime(date.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        if date is not None
        else np.nan
        for date in pd.to_datetime(df["last_updated"])
    ]
    log(df["last_updated"].unique())
    df = df[df["last_updated"] != "None"]
    df.dropna(
        subset=["last_updated", "temporal_coverage", "updated_frequency"], inplace=True
    )
    df["temporal_coverage"] = [
        k[0] if len(k) > 0 else k for k in df["temporal_coverage"]
    ]
    df.reset_index(drop=True, inplace=True)
    df.sort_values("last_updated", ascending=False, inplace=True)
    log(df[["dataset", "link", "last_updated"]])
    df = df[df.dataset.isin(selected_datasets)]
    df = df[
        df["last_updated"].apply(lambda x: x.timestamp())
        > (datetime.now() - pd.Timedelta(hours=hours)).timestamp()
    ]

    if not df.empty:
        os.system("mkdir -p /tmp/data/")
        df.to_csv("/tmp/data/updated_tables.csv")
        return True
    return False


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def message_last_tables() -> list:
    """
    Sends one tweet for each new table added recently. Uses 10 seconds interval for each new tweet
    """

    dataframe = pd.read_csv("/tmp/data/updated_tables.csv")
    datasets = dataframe.dataset.unique()

    for dataset in datasets:
        tables = dataframe[dataframe.dataset == dataset].table.to_list()
        coverages = dataframe[dataframe.dataset == dataset].temporal_coverage.to_list()
        updated_frequencies = dataframe[
            dataframe.dataset == dataset
        ].updated_frequency.to_list()
        links = dataframe[dataframe.dataset == dataset].link.to_list()
        main_tweet = f"""📣 O conjunto #{dataset} foi atualizado no datalake da @basedosdados.\n\nAcesse por aqui ⤵️\n{links[0]}
        """
        thread = "As tabelas atualizadas foram:\n"

        dict_frequency = {
            "day": "diários",
            "month": "anuais",
            "one_year": "anuais",
            "two_years": "bianuais",
            "recurring": "recorrentes",
        }
        i = 1
        for table, coverage, updated_frequency in zip(
            tables, coverages, updated_frequencies
        ):
            if len(coverage.split("(")[0]) == 4:
                thread = (
                    thread
                    + f"{str(i)+')'} {table}. Esses dados são {dict_frequency[updated_frequency]} e agora cobrem o período entre {coverage.split('(')[0]} e {coverage.split(')')[1]}\n"
                )
            elif len(coverage.split("(")[0]) == 7:
                thread = (
                    thread
                    + f"{str(i)+')'} {table}. Esses dados são {dict_frequency[updated_frequency]} e agora cobrem o período entre {coverage.split('(')[0]} e {coverage.split(')')[1]}\n"
                )
            elif len(coverage.split("(")[0]) == 10:
                thread = (
                    thread
                    + f"{str(i)+')'} {table}. Esses dados são {dict_frequency[updated_frequency]} e agora cobrem o período entre {coverage.split('(')[0]} e {coverage.split(')')[1]}\n"
                )
            else:
                raise ValueError(
                    f"Coverage information {coverage} doesn't matchs the BD's standard."
                )
            i += 1

        next_tweets = thread.split("\n")
        next_tweets = [tweet for tweet in next_tweets if len(tweet) > 0]
        texts = (
            [main_tweet] + [next_tweets[0] + "\n" + next_tweets[1]] + next_tweets[2:]
        )

        return texts


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def message_inflation_plot(dataset_id: str, table_id: str) -> list:
    """
    Creates an update plot based on table_id data.
    """
    os.system("mkdir -p /tmp/plots/")

    blobs = get_storage_blobs(dataset_id="br_ibge_ipca", table_id="mes_brasil")

    if len(blobs) != 0:
        dfs = []
        for blob in blobs:
            url_data = blob.public_url
            df = pd.read_csv(url_data, dtype={"id": str})
            dfs.append(df)

    # pylint: disable=W0108
    df["date"] = (
        df["ano"].apply(lambda x: str(x))
        + "-"
        + df["mes"].apply(lambda x: str(x).zfill(2))
    ).apply(lambda x: datetime.strptime(x, "%Y-%m"))
    dict_month = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }
    df["date"] = df["date"].map(dict_month)
    df.sort_values("date", inplace=True)
    last_data = df.iloc[-1, :]["variacao_doze_meses"]
    last_month = df.iloc[-1, :]["mes"]
    indice = "IPCA"

    texts = [
        f"Em {last_month}, a inflação acumulada nos últimos 12 meses medida pelo {indice} foi de {last_data}"
    ]

    print(last_data)
    df.set_index("date", inplace=True)
    df = df[df.index.year.isin(list(range(2015, 2022)))]

    sns.set_style("whitegrid")

    fig, ax = plt.subplots()

    ax.plot(df["variacao_doze_meses"], color="lime")
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    ax.set_title("Inflação acumulada - 12 meses (IPCA)", fontsize=14)

    filepath = "/tmp/plots/inflation.jpeg"

    fig.savefig(filepath, bbox_inches="tight")

    return texts


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def send_thread(
    access_token: str,
    access_token_secret: str,
    consumer_key: str,
    consumer_secret: str,
    bearer_token: str,
    texts: list,
    is_reply: bool,
    reply_id: None,
) -> int:
    """
    Sends a sequence of tweets at once.
    """

    if any(len(text) > 280 for text in texts):
        raise ValueError("Each tweet text must be 280 characters length at most.")

    client = tweepy.Client(
        bearer_token=bearer_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )

    for i, text in enumerate(texts):
        if i == 0:
            if is_reply:
                reply = client.create_tweet(text=text, in_reply_to_tweet_id=reply_id)
            else:
                reply = client.create_text(text=text)
        else:
            reply = client.create_text(
                text=text,
                in_reply_to_tweet_id=reply.data["id"],
            )

    return reply.data["id"]


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def send_media(
    access_token: str,
    access_token_secret: str,
    consumer_key: str,
    consumer_secret: str,
    bearer_token: str,
    text: str,
    images: list,
) -> int:
    """
    Sends a single tweet with a list of medias.
    """
    client = tweepy.Client(
        bearer_token=bearer_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )

    dict_medias = []
    for i, image in enumerate(images):
        media = client.media_upload(image)
        dict_medias.update({f"media_{i}": media})

    tweet = client.create_tweet(text=text, media=[list(dict_medias.values())])

    return tweet.data["id"]
