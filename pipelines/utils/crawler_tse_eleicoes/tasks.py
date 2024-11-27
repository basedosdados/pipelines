# -*- coding: utf-8 -*-
"""
Tasks for br_tse_eleicoes
"""

# pylint: disable=invalid-name,line-too-long

from prefect import task
from datetime import datetime, timedelta

from pipelines.utils.crawler_tse_eleicoes.utils import (
    flows_catalog,
)

from typing import TypeVar, Type

T = TypeVar('T')


@task
# Classes de formatação
def flows_control(table_id: str, mode: str) -> Type[T]:

  catalog =  flows_catalog()

  select_flow = catalog.get(table_id)["flow"]
  flow = select_flow(urls=catalog.get(table_id)["urls"], table_id=table_id,
                     source=catalog.get(table_id)["source"],
                     date_column_name=catalog.get(table_id)["date_column_name"],
                     date_format=catalog.get(table_id)["date_format"],
                     mode=mode)

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
def preparing_data(flow_class) -> tuple[str, str, str]:
   flow_class.formatar()
   return flow_class.path_output, flow_class.date_column_name, flow_class.date_format



