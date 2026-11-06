"""
Tasks for br_fgv_igp
"""

import csv
import pathlib

from prefect import task

from pipelines.datasets.br_fgv_igp.utils import IGPData
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


@task
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


@task
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


def _run_fgv_igp(
    dataset_id: str,
    table_id: str,
    indice: str,
    periodo: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    target: str,
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    # pyrefly: ignore [no-matching-overload]
    df_indice = crawler_fgv(indice, periodo)
    filepath = clean_fgv_df(df_indice)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
    )
    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
    )
    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )
