# -*- coding: utf-8 -*-
"""
Helper tasks that could fit any pipeline.
"""
# pylint: disable=C0103, C0301, invalid-name, E1101, R0913

import os
from pathlib import Path
from typing import Union

import basedosdados as bd
from prefect import task
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.utils import (
    dump_header_to_csv,
    log,
)


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
    try:
        if bucket_name in ["basedosdados-dev", "basedosdados"]:
            bd_version = bd.__version__
            log(f"USING BASEDOSDADOS {bd_version}")
            # pylint: disable=C0103
            tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
            table_staging = f"{tb.table_full_name['staging']}"
            # pylint: disable=C0103
            st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
            storage_path = f"{bucket_name}.staging.{dataset_id}.{table_id}"
            storage_path_link = f"https://console.cloud.google.com/storage/browser/{bucket_name}/staging/{dataset_id}/{table_id}"

            #####################################
            #
            # MANAGEMENT OF TABLE CREATION
            #
            #####################################
            log(f"DATA PATH -> {data_path}")
            log(f"LISTDIR -> {os.listdir(data_path)}")
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
                        mode="staging",
                        bucket_name=bucket_name,
                        not_found_ok=True,
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
                        mode="staging",
                        bucket_name=bucket_name,
                        not_found_ok=True,
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
                    mode="staging", bucket_name=bucket_name, not_found_ok=True
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

    except Exception as e:
        raise ValueError(
            f"Bucket name {e} is not valid. "
            "Please use basedosdados or basedosdados-dev"
        )


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
    create_table_and_upload_to_gcs_teste(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        bucket_name=bucket_name,
        wait=wait,
    )
    log(
        f"Materialization flow in {target} for {dataset_id}.{table_id} started."
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

    return None
