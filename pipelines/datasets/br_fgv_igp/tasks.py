# -*- coding: utf-8 -*-
"""
Tasks for br_fgv_igp
"""
# pylint: disable=invalid-name

import csv
import pathlib

from prefect import task

from pipelines.datasets.br_fgv_igp.utils import IGPData


@task  # noqa
def crawler_fgv(code: tuple, period: str) -> IGPData:
    """
    Crawl data from ipeadata

    Args:
        code (str): the Ipeadata codes key to the dict from constants
        period (str): the period of the data [month|year]

    Returns:
        pd.DataFrame: raw DataFrame from Ipeadata
    """
    ipea_data = IGPData(code, period)
    return ipea_data


@task  # noqa
def clean_fgv_df(igp_data: IGPData) -> pathlib.Path:
    """
    Clean FGV results

    Args:
        igp_data: the IGPData object

    Returns:
        str: the path of the csv file from DataFrame
    """
    filepath = pathlib.Path(igp_data.filepath)
    filedir = filepath.parents[0]

    if not filedir.is_dir():
        filedir.mkdir(parents=True, exist_ok=False)

    df = igp_data.df

    df.to_csv(
        filepath,
        encoding="utf-8",
        sep=",",
        decimal=".",
        na_rep="",
        quoting=csv.QUOTE_NONNUMERIC,
        index=False,
        header=True,
    )

    return filepath
