# -*- coding: utf-8 -*-
import asyncio
import datetime
import itertools
import time

import basedosdados as bd
import httpx
import numpy as np
import pandas as pd
from lxml import html
from prefect import task

from pipelines.datasets.br_cnj_improbidade_administrativa.utils import (
    PeopleInfoResponse,
    PeopleLine,
    ProcessInfoResponse,
    SentenceResponse,
    build_condenacao_url,
    build_home_url_page,
    build_people_info_url,
    build_process_url,
    get_async,
    normalize_string,
    parse_people_data,
    parse_peoples,
    parse_process,
    parse_sentence,
)
from pipelines.utils.utils import log


def get_number_pages() -> int:
    response = httpx.get(build_home_url_page(0))

    tree = html.fromstring(response.content)

    nodes: list = tree.xpath("table[2]//b[2]")

    assert len(nodes) == 1, f"Expected one node, but found {len(nodes)}"

    return int(nodes[0].text)


async def crawler_home_page(total_pages: int) -> list[httpx.Response]:

    pages_urls = [build_home_url_page(i) for i in range(0, total_pages)]

    max_connections = 3
    timeout = httpx.Timeout(30, pool=3.0)
    limits = httpx.Limits(max_connections=max_connections)
    semaphore = asyncio.Semaphore(max_connections)

    async def wrapper(co):
        async with semaphore:
            return await co

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        return await asyncio.gather(
            *[wrapper(get_async(client, url)) for url in pages_urls]
        )


async def crawler_sentences(
    peoples_info: list[PeopleLine],
) -> list[SentenceResponse]:
    condenacao_ids: list[str] = np.unique([i["condenacao_id"] for i in peoples_info])

    max_connections = 3
    timeout = httpx.Timeout(30, pool=3.0)
    limits = httpx.Limits(max_connections=max_connections)
    semaphore = asyncio.Semaphore(max_connections)

    async def wrapper(client, sentence_id):
        async with semaphore:
            response = await get_async(client, build_condenacao_url(sentence_id))
            return {"condenacao_id": sentence_id, "response": response}

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        return await asyncio.gather(*[wrapper(client, sentence_id) for sentence_id in condenacao_ids])  # type: ignore


async def get_peoples_info(ids: list[tuple[str, str]]) -> list[PeopleInfoResponse]:
    max_connections = 5
    timeout = httpx.Timeout(30, pool=3.0, read=None)
    semaphore = asyncio.Semaphore(max_connections)
    limits = httpx.Limits(max_connections=max_connections)

    async def wrapper(client, sentence_id, people_id):
        async with semaphore:
            response = await get_async(
                client, build_people_info_url(sentence_id, people_id)
            )
            return {"condenacao_id": sentence_id, "response": response}

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        return await asyncio.gather(
            *[
                wrapper(client, sentence_id, people_id)
                for (sentence_id, people_id) in ids
            ]  # type: ignore
        )


async def crawler_process(peoples: list[PeopleLine]) -> list[ProcessInfoResponse]:
    max_connections = 5
    timeout = httpx.Timeout(30, pool=3.0, read=None)
    semaphore = asyncio.Semaphore(max_connections)
    limits = httpx.Limits(max_connections=max_connections)

    async def wrapper(client: httpx.AsyncClient, people: PeopleLine):
        async with semaphore:
            response = await get_async(client, build_process_url(people["processo_id"]))
            return {"process_id": people["processo_id"], "response": response}

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        return await asyncio.gather(
            *[wrapper(client, people_line) for people_line in peoples]  # type: ignore
        )


async def run_async(total_pages: int) -> pd.DataFrame:

    time_start_home_page = time.time()
    requests_home_page = await crawler_home_page(total_pages)
    time_end_home_page = time.time()

    log("Crawler home page finished")

    parsed_main_list = list(
        itertools.chain(*[parse_peoples(i) for i in requests_home_page])
    )

    log("Starting crawler sentences")
    time_start_get_sentences = time.time()
    sentence_responses = await crawler_sentences(parsed_main_list)
    time_end_get_sentences = time.time()

    log("Crawler sentences finished")

    parsed_sentences = [parse_sentence(i) for i in sentence_responses]

    valid_info_peoples = [
        (sentence["condenacao_id"], sentence["pessoa_id"])  # type: ignore
        for sentence in parsed_sentences
        if "pessoa_id" in sentence  # type: ignore
    ]

    log("Starting get peoples info data")

    time_start_get_info_peoples = time.time()
    peoples_info_responses = await get_peoples_info(valid_info_peoples)
    time_end_get_info_peples = time.time()

    log(f"Get peoples info finished: {len(peoples_info_responses)}")

    parsed_peoples_info = [parse_people_data(i) for i in peoples_info_responses]

    time_start_crawler_process = time.time()
    process_responses = await crawler_process(parsed_main_list)
    time_end_crawler_process = time.time()

    parsed_process = [parse_process(i) for i in process_responses]

    log(f"Crawler home page. Time {time_end_home_page - time_start_home_page}")
    log(f"Crawler sentences. Time {time_end_get_sentences - time_start_get_sentences}")
    log(
        f"Crawler peoples infos. Time {time_end_get_info_peples - time_start_get_info_peoples}"
    )
    log(
        f"Crawler process. Time {time_end_crawler_process - time_start_crawler_process}"
    )

    df_main_list = pd.DataFrame(parsed_main_list)
    df_main_list["condenacao_id"] = df_main_list["condenacao_id"].astype("string")

    df_parsed_peoples_info = pd.DataFrame(parsed_peoples_info)
    df_parsed_peoples_info["condenacao_id"] = df_parsed_peoples_info[
        "condenacao_id"
    ].astype("string")

    df_process = pd.DataFrame(parsed_process)
    df_process["processo_id"] = df_process["processo_id"].astype("string")

    df_senteces = pd.DataFrame(parsed_sentences)
    df_senteces["condenacao_id"] = df_senteces["condenacao_id"].astype("string")

    log(f"Dataframe sentences columns: {df_senteces.columns}")

    new_columns_setences = {
        name: normalize_string(name.replace(":", "").replace("?", ""))
        for name in df_senteces.columns
    }

    df_senteces = df_senteces.rename(columns=new_columns_setences, errors="raise")

    return (
        df_main_list.merge(
            df_parsed_peoples_info, left_on="condenacao_id", right_on="condenacao_id"
        )
        .merge(df_senteces, left_on="condenacao_id", right_on="condenacao_id")
        .merge(df_process, left_on="processo_id", right_on="processo_id")
    )


@task
def main_task() -> pd.DataFrame:
    pages = get_number_pages()
    return asyncio.run(run_async(pages))


@task
def is_up_to_date() -> bool:
    number_lines_bq = bd.read_sql(
        query="select count(*) as n from `basedosdados.br_cnj_improbidade_administrativa.condenacao`",
        from_file=True,
        billing_project_id="basedosdados-dev",
    )
    value_from_bq = number_lines_bq["n"][0]  # type: ignore

    log(f"Total lines on BigQuery: {value_from_bq}")

    response = httpx.get(build_home_url_page(0))

    tree = html.fromstring(response.content)

    node: list = tree.xpath(".//table[2]//tr//td[1]")

    if len(node) == 0:
        raise Exception("Failed to get element from xpath")

    text = node[0].text.strip()

    _, number = text.split(":")

    log(f"Total Peoples: {int(number)}")

    return value_from_bq == int(number)


@task
def get_max_date(df: pd.DataFrame) -> datetime.date:
    max_date: datetime.date = df["data_propositura"].max()
    return max_date


@task
def write_csv_file(df: pd.DataFrame) -> str:
    path = "/tmp/data.csv"
    df.to_csv(path, index=False)
    return path
