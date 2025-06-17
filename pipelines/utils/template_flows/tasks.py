# -*- coding: utf-8 -*-
"""
register flow
"""

import os
from pathlib import Path
from typing import Union

import basedosdados as bd
from prefect import task
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.template_flows.constants import (
    constants as template_constants,
)
from pipelines.utils.utils import (
    dump_header_to_csv,
    log,
)


def modify_file_credentials():
    log(f"CREDENTIALS -> {os.listdir('/root/.basedosdados/credentials/')}")
    log(f".BASEDOSDADOS -> {os.listdir('/root/.basedosdados/')}")
    log(f"TEMPLATES -> {os.listdir('/root/.basedosdados/templates/')}")


@task
def create_table_and_upload_to_gcs_teste(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    bucket_name: str,
    source_format: str = "csv",
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Create table using BD+ and upload to GCS.
    """

    modify_file_credentials()
    data_path = Path(data_path)
    log(f"Data path: {data_path}")
    bd_version = bd.__version__
    log(f"USING BASEDOSDADOS {bd_version}")
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    table_staging = f"{tb.table_full_name['staging']}"
    # pylint: disable=C0103
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    storage_path_link = f"https://console.cloud.google.com/storage/browser/{bucket_name}/staging/{dataset_id}/{table_id}"

    #####################################
    #
    # MANAGEMENT OF TABLE CREATION
    #
    #####################################
    log("STARTING TABLE CREATION MANAGEMENT")
    if dump_mode == "append":
        if tb.table_exists(mode="staging"):
            log(
                f"MODE APPEND: Table ALREADY EXISTS:\n{table_staging}\n{storage_path_link}"
            )
        else:
            # the header is needed to create a table when dosen't exist
            log(
                "MODE APPEND: Table DOSEN'T EXISTS\n"
                + "Start to CREATE HEADER file"
            )
            header_path = dump_header_to_csv(
                data_path=data_path, source_format=source_format
            )
            log(f"MODE APPEND: Created HEADER file:\n{header_path}")

            tb.create(
                path=header_path,
                if_storage_data_exists="replace",
                if_table_exists="replace",
                source_format=source_format,
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
    elif dump_mode == "overwrite":
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
        log(
            "MODE OVERWRITE: Table DOSEN'T EXISTS\n"
            + "Start to CREATE HEADER file"
        )
        header_path = dump_header_to_csv(
            data_path=data_path, source_format=source_format
        )
        log(f"MODE OVERWRITE: Created HEADER file:\n{header_path}")

        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_exists="replace",
            source_format=source_format,
        )

        log(
            f"MODE OVERWRITE: Sucessfully CREATED TABLE\n{table_staging}\n{storage_path_link}"
        )

        st.delete_table(
            mode="staging", bucket_name=st.bucket_name, not_found_ok=True
        )
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
    log(f"data_path -> {data_path}")
    log(f"data_path exists? {data_path.exists()}")
    log(f"data_path is dir? {data_path.is_dir()}")
    log(f"data_path is file? {data_path.is_file()}")
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
        log(
            "STEP UPLOAD: Table does not exist in STAGING, need to create first"
        )


# @task
# def create_table_and_upload_to_gcs_teste(
#     data_path: Union[str, Path],
#     dataset_id: str,
#     table_id: str,
#     dump_mode: str,
#     bucket_name: str,
#     source_format: str = "csv",
#     wait=None,  # pylint: disable=unused-argument
# ) -> None:
#     """
#     Create table using BD+ and upload to GCS.
#     """
#     log(f"BUCKET_NAME -> {bucket_name}")
#     log(f"DATASET_ID -> {dataset_id}")
#     log(f"TABLE_ID -> {table_id}")
#     log(f"Verificando o path do arquivo: {data_path}")
#     log(f"Arquivo existe? {Path(data_path).exists()}")
#     log(f"DUMP_MODE -> {dump_mode}")
#     log(f"SOURCE_FORMAT -> {source_format}")

#     try:
#         if bucket_name in ["basedosdados-dev", "basedosdados"]:
#             bd_version = bd.__version__
#             log(f"USING BASEDOSDADOS {bd_version}")
#             # pylint: disable=C0103
#             tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
#             log(f"TB -> {tb}")
#             table_staging = f"{tb.table_full_name['staging']}"
#             log(f"TABLE_STAGING -> {table_staging}")
#             # pylint: disable=C0103
#             st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
#             storage_path = f"{bucket_name}.staging.{dataset_id}.{table_id}"
#             storage_path_link = f"https://console.cloud.google.com/storage/browser/{bucket_name}/staging/{dataset_id}/{table_id}"

#             #####################################
#             #
#             # MANAGEMENT OF TABLE CREATION
#             #
#             #####################################
#             # log(f"DATA PATH -> {data_path}")
#             log("STARTING TABLE CREATION MANAGEMENT")
#             if dump_mode == "append":
#                 if tb.table_exists(mode="staging"):
#                     log(
#                         f"MODE APPEND: Table ALREADY EXISTS:\n{table_staging}\n{storage_path_link}"
#                     )

#                 else:
#                     # the header is needed to create a table when dosen't exist
#                     log(
#                         "MODE APPEND: Table DOSEN'T EXISTS\n"
#                         + "Start to CREATE HEADER file"
#                     )
#                     header_path = dump_header_to_csv(
#                         data_path=data_path, source_format=source_format
#                     )
#                     log(f"MODE APPEND: Created HEADER file:\n{header_path}")

#                     tb.create(
#                         path=header_path,
#                         if_storage_data_exists="replace",
#                         if_table_exists="replace",
#                         source_format=source_format,
#                     )

#                     log(
#                         "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
#                         f"{table_staging}\n"
#                         f"{storage_path_link}"
#                     )  # pylint: disable=C0301

#                     st.delete_table(
#                         mode="staging",
#                         bucket_name=bucket_name,
#                         not_found_ok=True,
#                     )
#                     log(
#                         "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
#                         f"{storage_path}\n"
#                         f"{storage_path_link}"
#                     )  # pylint: disable=C0301
#             elif dump_mode == "overwrite":
#                 if tb.table_exists(mode="staging"):
#                     log(
#                         "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
#                         f"{storage_path}\n"
#                         f"{storage_path_link}"
#                     )  # pylint: disable=C0301
#                     st.delete_table(
#                         mode="staging",
#                         bucket_name=bucket_name,
#                         not_found_ok=True,
#                     )
#                     log(
#                         "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
#                         f"{storage_path}\n"
#                         f"{storage_path_link}"
#                     )  # pylint: disable=C0301
#                     tb.delete(mode="all")
#                     log(
#                         "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
#                         f"{table_staging}\n"
#                         f"{tb.table_full_name['prod']}"
#                     )  # pylint: disable=C0301

#                 # the header is needed to create a table when dosen't exist
#                 # in overwrite mode the header is always created
#                 log(
#                     "MODE OVERWRITE: Table DOSEN'T EXISTS\n"
#                     + "Start to CREATE HEADER file"
#                 )
#                 header_path = dump_header_to_csv(
#                     data_path=data_path, source_format=source_format
#                 )
#                 log(f"MODE OVERWRITE: Created HEADER file:\n{header_path}")

#                 tb.create(
#                     path=header_path,
#                     if_storage_data_exists="replace",
#                     if_table_exists="replace",
#                     source_format=source_format,
#                 )

#                 log(
#                     f"MODE OVERWRITE: Sucessfully CREATED TABLE\n{table_staging}\n{storage_path_link}"
#                 )

#                 st.delete_table(
#                     mode="staging", bucket_name=bucket_name, not_found_ok=True
#                 )
#                 log(
#                     f"MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
#                     f"{storage_path}\n"
#                     f"{storage_path_link}"
#                 )  # pylint: disable=C0301

#             #####################################
#             #
#             # Uploads a bunch of CSVs using BD+
#             #
#             #####################################

#             log("STARTING UPLOAD TO GCS")
#             if tb.table_exists(mode="staging"):
#                 # the name of the files need to be the same or the data doesn't get overwritten
#                 log(os.listdir(data_path))
#                 tb.append(filepath=data_path, if_exists="replace")

#                 log(
#                     f"STEP UPLOAD: Successfully uploaded {data_path} to Storage:\n"
#                     f"{storage_path}\n"
#                     f"{storage_path_link}"
#                 )
#             else:
#                 # pylint: disable=C0301
#                 log(
#                     "STEP UPLOAD: Table does not exist in STAGING, need to create first"
#                 )

#     except Exception as e:
#         raise ValueError(
#             f"Bucket name {e} is not valid. "
#             "Please use basedosdados or basedosdados-dev"
#         )


@task
def template_upload_to_gcs_and_materialization(
    dataset_id: str,
    table_id: str,
    data_path: str,
    target: str,
    bucket_name: str,
    labels: str,
    dbt_alias: str = True,
    dump_mode: str = "append",
    run_model: str = "run/test",
    wait=None,
):
    create_upload_table_gcs = create_flow_run.run(
        flow_name=template_constants.FLOW_CREATE_UPLOAD_TABLE_GCS.value,
        project_name=constants.PREFECT_STAGING_PROJECT.value,
        parameters={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "data_path": data_path,
            "dump_mode": dump_mode,
            "bucket_name": bucket_name,
            "source_format": "csv",
            "wait": wait,
        },
        labels=labels,
        run_name=f"Create and Upload {dataset_id}.{table_id} to GCS",
    )

    wait_for_flow_run.run(
        create_upload_table_gcs,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    materialization_flow = create_flow_run.run(
        flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
        project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        parameters={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "target": target,
            "dbt_command": run_model,
            "dbt_alias": dbt_alias,
            "disable_elementary": False,
            "download_csv_file": False,
        },
        labels=labels,
        run_name=f"Materialize {dataset_id}.{table_id}",
    )

    wait_for_flow_run.run(
        materialization_flow,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )
    log(
        f"[INFO] Template flow to create table and upload to GCS completed for {dataset_id}.{table_id}."
    )
