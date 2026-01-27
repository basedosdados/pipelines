"""
Tasks for br_tse_eleicoes
"""

from datetime import datetime, timedelta
from typing import TypeVar

from prefect import task

from pipelines.crawler.tse_eleicoes.utils import (
    flows_catalog,
)

T = TypeVar("T")


@task
# Classes de formatação
def flows_control(table_id: str, mode: str) -> type[T]:
    catalog = flows_catalog()

    select_flow = catalog.get(table_id)["flow"]
    flow = select_flow(
        urls=catalog.get(table_id)["urls"],
        table_id=table_id,
        source=catalog.get(table_id)["source"],
        mode=mode,
    )

    return flow


@task(
    max_retries=1,
    retry_delay=timedelta(seconds=60),
)
def get_data_source_max_date(flow_class) -> datetime:
    flow_class.download_urls()
    date = flow_class.get_data_source_max_date()
    return date


@task
def preparing_data(flow_class) -> str:
    flow_class.formatar()
    return flow_class.path_output
