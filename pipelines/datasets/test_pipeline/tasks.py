# -*- coding: utf-8 -*-
"""
Tasks for basedosdados
"""
# pylint: disable=line-too-long, W0702, E1101, C0103

import datetime
import os
from pathlib import Path
from typing import Union


import basedosdados as bd
import numpy as np
import pandas as pd
from prefect import task
import requests

from pipelines.utils.utils import log


@task(nout=2)
def get_random_expression() -> pd.DataFrame:
    """
    Get random data
    """
    URL = "https://x-math.herokuapp.com/api/random"
    response = requests.get(URL, timeout=5)

    cols = ["date", "first", "second", "operation", "expression", "answer"]
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        dataframe = pd.json_normalize(response.json())
        dataframe["date"] = time_stamp
    except Exception:
        data = [datetime.datetime.now(), np.nan, np.nan, np.nan, np.nan, np.nan]
        dataframe = pd.DataFrame(data, columns=cols)
    return dataframe[cols], time_stamp


@task
def dataframe_to_csv(
    dataframe: pd.DataFrame, path: Union[str, Path], time_stamp
) -> Union[str, Path]:
    """
    Writes a dataframe to a CSV file.
    """
    # Remove filename from path
    path = Path(path)
    # Create directory if it doesn't exist
    os.makedirs(path, exist_ok=True)
    # Write dataframe to CSV
    log(f"Writing dataframe to CSV: {path}")
    time_stamp = time_stamp.replace(" ", "_").replace(":", "_").replace("-", "_")
    dataframe.to_csv(path / f"{time_stamp}.csv", index=False)
    log(f"Wrote dataframe to CSV: {path}")

    return path


@task
def upload_to_gcs(path: Union[str, Path], dataset_id: str, table_id: str) -> None:
    """
    Uploads a bunch of CSVs using BD+
    """
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    # st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    if tb.table_existime_stamp(mode="staging"):
        # Delete old data
        # st.delete_table(
        #     mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        # log(
        #     f"Successfully deleted OLD DATA {st.bucket_name}.staging.{dataset_id}.{table_id}"
        # )

        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(
            filepath=path,
            if_existime_stamp="replace",
        )

        log(
            f"Successfully uploaded {path} to {tb.bucket_name}.staging.{dataset_id}.{table_id}"
        )

    else:
        log(
            "Table does not exist in STAGING, need to create it in local first.\nCreate and publish the table in BigQuery first."
        )
