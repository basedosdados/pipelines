# -*- coding: utf-8 -*-
"""
Tasks for world_sofascore_competicoes_futebol
"""

# pylint: disable=invalid-name,line-too-long

from datetime import timedelta

from prefect import task

from pipelines.crawler.world_sofascore_competicoes_futebol.utils import (
    preparing_data_and_max_date,
)


@task(
    max_retries=1,
    retry_delay=timedelta(seconds=60),
)
def get_data_source_max_date_and_preparing_data(
    table_id: str,
) -> tuple[str, str]:
    return preparing_data_and_max_date(table_id)
