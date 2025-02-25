# -*- coding: utf-8 -*-
"""
Tasks for br_tse_eleicoes
"""

# pylint: disable=invalid-name,line-too-long

from datetime import datetime, timedelta

from prefect import task

from pipelines.utils.crawler_world_sofascore_competicoes_futebol.utils import (
    extract_all_years,
)


@task(
    max_retries=1,
    retry_delay=timedelta(seconds=60),
)
def get_data_source_max_date() -> datetime:
    return datetime.today()


@task
def preparing_data(table_id: str) -> None:
    extract_all_years(table_id)
