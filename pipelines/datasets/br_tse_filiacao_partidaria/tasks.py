# -*- coding: utf-8 -*-
"""
Tasks for br_ms_cnes
"""

from datetime import datetime

from prefect import task

from pipelines.datasets.br_rf_cafir.constants import constants as br_rf_cafir_constants
from pipelines.datasets.br_tse_filiacao_partidaria.utils import (
    PartidariaColetaTramento
)
from pipelines.utils.utils import log

from typing import TypeVar, Type

T = TypeVar('T')

@task
def collector_starter() -> Type[T]:
    collector = PartidariaColetaTramento()
    return collector

@task
def get_data_source_max_date() -> datetime:
   return datetime.today()

@task
def collect(collector) -> None:
    collector.coleta()


@task
def processing(collector) -> str:
    collector.merge_segments()
    collector.processing()
    return collector.path_base_output
