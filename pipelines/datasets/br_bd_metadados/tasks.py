# -*- coding: utf-8 -*-
"""
Tasks for br_bd_metadados
"""
from datetime import datetime, timedelta

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.utils.utils import log



@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler():
    log("empty dataset")
    return pd.DataFrame()