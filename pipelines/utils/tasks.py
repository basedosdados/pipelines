# -*- coding: utf-8 -*-
"""
Helper tasks that could fit any pipeline.
"""
# pylint: disable=C0103, C0301, invalid-name, E1101, R0913

from datetime import timedelta, datetime
from pathlib import Path
from typing import Union
import inspect
import textwrap


import basedosdados as bd
import prefect
from prefect import task
from prefect.client import Client
import ruamel.yaml as ryaml
import pandas as pd

from pipelines.constants import constants
from pipelines.utils.utils import (
    get_credentials_from_secret,
    log,
    dump_header_to_csv,
)

##################
#
# Hashicorp Vault
#
##################


@task(checkpoint=False, nout=2)
def get_credentials(secret_path: str):
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    return get_credentials_from_secret(secret_path)


###############
#
# Upload to GCS
#
###############


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def create_table_and_upload_to_gcs(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_type: str,
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Create table using BD+ and upload to GCS.
    """
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    table_staging = f"{tb.table_full_name['staging']}"
    # pylint: disable=C0103
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    storage_path_link = f"https://console.cloud.google.com/storage/browser/{st.bucket_name}/staging/{dataset_id}/{table_id}"

    #####################################
    #
    # MANAGEMENT OF TABLE CREATION
    #
    #####################################
    log("STARTING TABLE CREATION MANAGEMENT")
    if dump_type == "append":
        if tb.table_exists(mode="staging"):
            log(
                f"MODE APPEND: Table ALREADY EXISTS:"
                f"\n{table_staging}"
                f"\n{storage_path_link}"
            )
        else:
            # the header is needed to create a table when dosen't exist
            log("MODE APPEND: Table DOSEN'T EXISTS\n" "Start to CREATE HEADER file")
            header_path = dump_header_to_csv(data_path=data_path)
            log("MODE APPEND: Created HEADER file:\n" f"{header_path}")

            tb.create(
                path=header_path,
                if_storage_data_exists="replace",
                if_table_config_exists="replace",
                if_table_exists="replace",
            )

            log(
                "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                f"{table_staging}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301

            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
    elif dump_type == "overwrite":
        if tb.table_exists(mode="staging"):
            log(
                "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            tb.delete(mode="all")
            log(
                "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
                f"{table_staging}\n"
                f"{tb.table_full_name['prod']}"
            )  # pylint: disable=C0301

        # the header is needed to create a table when dosen't exist
        # in overwrite mode the header is always created
        log("MODE OVERWRITE: Table DOSEN'T EXISTS\n" "Start to CREATE HEADER file")
        header_path = dump_header_to_csv(data_path=data_path)
        log("MODE OVERWRITE: Created HEADER file:\n" f"{header_path}")

        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_config_exists="replace",
            if_table_exists="replace",
        )

        log(
            "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
            f"{table_staging}\n"
            f"{storage_path_link}"
        )

        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        log(
            f"MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )  # pylint: disable=C0301

    #####################################
    #
    # Uploads a bunch of CSVs using BD+
    #
    #####################################

    log("STARTING UPLOAD TO GCS")
    if tb.table_exists(mode="staging"):
        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(filepath=data_path, if_exists="replace")

        log(
            f"STEP UPLOAD: Successfully uploaded {data_path} to Storage:\n"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )
    else:
        # pylint: disable=C0301
        log("STEP UPLOAD: Table does not exist in STAGING, need to create first")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
# pylint: disable=R0914
def update_metadata(dataset_id: str, table_id: str, fields_to_update: list) -> None:
    """
    Update metadata for a selected table

    dataset_id: dataset_id,
    table_id: table_id,
    fields_to_update: list of dictionaries with key and values to be updated
    """
    # add credentials to config.toml
    (api_key, url) = get_credentials_from_secret(secret_path="ckan_credentials")

    handle = bd.Metadata(dataset_id=dataset_id, table_id=table_id)
    handle.create(if_exists="replace")

    handle.CKAN_API_KEY = api_key
    handle.CKAN_URL = url

    yaml = ryaml.YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=4, sequence=6, offset=4)

    config_file = handle.filepath.as_posix()

    with open(config_file, encoding="utf-8") as fp:
        data = yaml.load(fp)

    # this is, of course, very slow but very few fields will be update each time, so the cubic algo will not have major performance consequences
    for field in fields_to_update:
        for k, v in field.items():
            if isinstance(v, dict):
                for i, j in v.items():
                    data[k][i] = j
            else:
                data[k] = v

    with open(config_file, "w", encoding="utf-8") as fp:
        yaml.dump(data, fp)

    if handle.validate():
        handle.publish(if_exists="replace")
        log(f"Metadata for {table_id} updated")
    else:
        log("Fail to validate metadata.")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def publish_table(
    path: Union[str, Path],  # pylint: disable=unused-argument
    dataset_id: str,
    table_id: str,
    if_exists="raise",
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """Creates BigQuery table at production dataset.
    Table should be located at `<dataset_id>.<table_id>`.
    It creates a view that uses the query from
    `<metadata_path>/<dataset_id>/<table_id>/publish.sql`.
    Make sure that all columns from the query also exists at
    `<metadata_path>/<dataset_id>/<table_id>/table_config.sql`, including
    the partitions.
    Args:
        if_exists (str): Optional.
            What to do if table exists.
            * 'raise' : Raises Conflict exception
            * 'replace' : Replace table
            * 'pass' : Do nothing
    Todo:
        * Check if all required fields are filled
    """

    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    if if_exists == "replace":
        tb.delete(mode="prod")

    tb.client["bigquery_prod"].query(
        (tb.table_folder / "publish.sql").open("r", encoding="utf-8").read()
    ).result()

    tb.update()

    if tb.table_exists(mode="prod"):
        log(f"Successfully uploaded {table_id} to prod.")
    else:

        log("I cannot upload {table_id} in prod.")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_temporal_coverage(
    filepath: str, date_cols: list, time_unit: str, interval: str
) -> str:
    """
    Generates a temporal coverage string from a csv.
    The pattern follows the BD's Style Manual (https://basedosdados.github.io/mais/style_data/#cobertura-temporal)

    args:
    filepath: csv filepath
    date_cols: date columns to use as reference (use the order [year, month, day])
    time_unit: day | month | year
    interval: time between dates.
    For example, if the time_unit is month and the data is update every quarter, then the intervel is 3.
    """
    if len(date_cols) == 1:
        date_col = date_cols[0]
        df = pd.read_csv(filepath, usecols=[date_col], parse_dates=[date_col])
        dates = df[date_col].to_list()
        dates.sort()
    elif len(date_cols) == 2:
        year = date_cols[0]
        month = date_cols[1]
        df = pd.read_csv(filepath, usecols=[year, month])
        df["date"] = [
            datetime.strptime(str(x) + "-" + str(y) + "-" + "1", "%Y-%m-%d")
            for x, y in zip(df[year], df[month])
        ]
        dates = df["date"].to_list()
        dates.sort()
    elif len(date_cols) == 3:
        year = date_cols[0]
        month = date_cols[1]
        day = date_cols[2]
        df = pd.read_csv(filepath, usecols=[year, month])
        df["date"] = [
            datetime.strptime(str(x) + "-" + str(y) + "-" + str(y), "%Y-%m-%d")
            for x, y in zip(df[year], df[month], df[day])
        ]
        dates = df["date"].to_list()
        dates.sort()
    else:
        raise ValueError(
            "date_cols must be a list with up to 3 elements in the following order [year, month, day]"
        )

    if time_unit == "day":
        start_date = (
            f"{dates[0].year}-{dates[0].strftime('%m')}-{dates[0].strftime('%d')}"
        )
        end_date = (
            f"{dates[-1].year}-{dates[-1].strftime('%m')}-{dates[-1].strftime('%d')}"
        )
        return start_date + "(" + interval + ")" + end_date
    if time_unit == "month":
        start_date = f"{dates[0].year}-{dates[0].strftime('%m')}"
        end_date = f"{dates[-1].year}-{dates[-1].strftime('%m')}"
        return start_date + "(" + interval + ")" + end_date
    if time_unit == "year":
        start_date = f"{dates[0].year}"
        end_date = f"{dates[-1].year}"
        return start_date + "(" + interval + ")" + end_date

    raise ValueError("time_unit must be one of the following: day, month, year")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def update_publish_sql(dataset_id: str, table_id: str, dtype: dict, columns: list):
    """Edit publish.sql with columns and bigquery_type"""

    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    # publish.sql header and instructions
    publish_txt = """
    /*
    Query para publicar a tabela.

    Esse é o lugar para:
    - modificar nomes, ordem e tipos de colunas
    - dar join com outras tabelas
    - criar colunas extras (e.g. logs, proporções, etc.)

    Qualquer coluna definida aqui deve também existir em `table_config.yaml`.

    # Além disso, sinta-se à vontade para alterar alguns nomes obscuros
    # para algo um pouco mais explícito.

    TIPOS:
    - Para modificar tipos de colunas, basta substituir STRING por outro tipo válido.
    - Exemplo: `SAFE_CAST(column_name AS NUMERIC) column_name`
    - Mais detalhes: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
    */
    """
    accepted_types = [
        "INT64",
        "NUMERIC",
        "FLOAT64",
        "BOOL",
        "STRING",
        "BYTES",
        "ARRAY",
        "DATE",
        "TIME",
        "DATETIME",
        "TIMESTAMP",
        "STRUCT",
        "GEOGRAPHY",
    ]
    if not set(dtype.values()).issubset(accepted_types):
        non_type = list(set(accepted_types) - set(dtype))[0]
        raise ValueError(
            f"Big Query types must be one of the following 'INT64', 'NUMERIC', 'FLOAT64', 'BOOL', 'STRING', 'BYTES', 'ARRAY', 'DATE', 'TIME', 'DATETIME', 'TIMESTAMP', 'STRUCT', 'GEOGRAPHY'.\n{non_type} was found"
        )

    # remove triple quotes extra space
    publish_txt = inspect.cleandoc(publish_txt)
    publish_txt = textwrap.dedent(publish_txt)

    # add create table statement
    project_id_prod = tb.client["bigquery_prod"].project
    publish_txt += (
        f"\n\nCREATE VIEW {project_id_prod}.{tb.dataset_id}.{tb.table_id} AS\nSELECT \n"
    )

    # sort columns by is_partition, partitions_columns come first

    # pylint: disable=W0212
    md = bd.Metadata(dataset_id=dataset_id, table_id=table_id)
    md.create(columns=columns, if_exists="replace")
    tb._make_publish_sql()

    columns = tb.table_config["columns"]

    # add columns in publish.sql
    for col in columns:
        name = col["name"]
        if name in dtype.keys():
            bigquery_type = dtype[name]
        else:
            if col["bigquery_type"] is None:
                bigquery_type = "STRING"
            else:
                bigquery_type = col["bigquery_type"].upper()

        publish_txt += f"SAFE_CAST({name} AS {bigquery_type}) {name},\n"
    # remove last comma
    publish_txt = publish_txt[:-2] + "\n"

    # add from statement
    project_id_staging = tb.client["bigquery_staging"].project
    publish_txt += (
        f"FROM {project_id_staging}.{tb.dataset_id}_staging.{tb.table_id} AS t"
    )

    # save publish.sql in table_folder
    (tb.table_folder / "publish.sql").open("w", encoding="utf-8").write(publish_txt)


# pylint: disable=W0613
@task
def rename_current_flow_run(msg: str, wait=None) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(flow_run_id, msg)


def rename_current_flow_run_dataset_table(
    prefix: str, dataset_id, table_id, wait=None
) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(flow_run_id, f"{prefix}{dataset_id}.{table_id}")


@task
def get_date_time_str(wait=None) -> str:
    """
    Get current time as string
    """
    return datetime.now().strftime("%Y-%m-%d %HH:%MM")
