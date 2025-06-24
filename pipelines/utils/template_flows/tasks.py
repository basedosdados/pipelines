import base64
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
    billing_project_id: str,
    source_format: str = "csv",
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Create table using BD+ and upload to GCS.
    """

    data_path = Path(data_path)
    log(f"Data path: {data_path}")
    log(
        f"[INFO] Utilizando bucket: {bucket_name} e projeto: {billing_project_id}"
    )
    bd_version = bd.__version__
    log(f"USING BASEDOSDADOS {bd_version}")
    tb = bd.Table(
        dataset_id=dataset_id,
        table_id=table_id,
        billing_project_id=billing_project_id,
    )
    st = bd.Storage(
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
        billing_project_id=billing_project_id,
    )
    storage_path = f"{bucket_name}.staging.{dataset_id}.{table_id}"
    storage_path_link = f"https://console.cloud.google.com/storage/browser/{bucket_name}/staging/{dataset_id}/{table_id}"

    #####################################
    #
    # MANAGEMENT OF TABLE CREATION
    #
    #####################################
    log("STARTING TABLE CREATION MANAGEMENT")
    if dump_mode == "append":
        if tb.table_exists(mode="staging"):
            log(f"MODE APPEND: Table ALREADY EXISTS:\n{storage_path_link}")
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

        log(f"MODE OVERWRITE: Sucessfully CREATED TABLE\n{storage_path_link}")

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


def _decode_env(env: str) -> str:
    """
    Decode environment variable
    """
    return base64.b64decode(os.getenv(env).encode("utf-8")).decode("utf-8")


# def create_credentials(config_path="/root/.basedosdados/", target=None):
#     log("Creating credentials and config file")
#     config_path = Path(config_path)
#     config_file = config_path / "config.toml"

#     if os.getenv("BASEDOSDADOS_CONFIG"):
#         log("Environment variable BASEDOSDADOS_CONFIG found.")
#         with open(config_file, "w", encoding="utf-8") as f:
#             f.write(_decode_env("BASEDOSDADOS_CONFIG"))

#         config_data = toml.load(config_file)

#         if target == "dev":
#             log("[INFO] Setting config.toml file to default values for dev.")
#             config_data["bucket_name"] = "basedosdados-dev"
#             config_data["gcloud-projects"]["staging"]["name"] = (
#                 "basedosdados-dev"
#             )
#             config_data["gcloud-projects"]["prod"]["name"] = "basedosdados-dev"

#             with open(config_file, "w") as toml_file:
#                 toml.dump(config_data, toml_file)

#         def use_config():
#             return toml.load(config_file)["bucket_name"]

#         bucket = use_config()

#         log(f"[DEBUG] Bucket definido após config: {bucket}")

#     else:
#         log("Environment variable BASEDOSDADOS_CONFIG not found.")


# def return_config_toml_default(
#     config_path="/root/.basedosdados/", target=None
# ):
#     """
#     Initialize config file
#     """
#     if target == "dev":
#         log("[INFO] Return config.toml file to default values.")
#         config_path = Path(config_path)
#         config_file = config_path / "config.toml"
#         if os.getenv("BASEDOSDADOS_CONFIG"):
#             log("Environment variable BASEDOSDADOS_CONFIG found.")
#             log(f"Config file path: {config_file}")
#             with open(config_file, "w", encoding="utf-8") as f:
#                 f.write(_decode_env("BASEDOSDADOS_CONFIG"))
#             with open(config_file, "r") as toml_file:
#                 config_data = toml.load(toml_file)
#                 log(config_data)

#                 config_data["bucket_name"] = "basedosdados"
#                 config_data["gcloud-projects"]["staging"]["name"] = (
#                     "basedosdados-staging"
#                 )
#                 config_data["gcloud-projects"]["prod"]["name"] = "basedosdados"

#                 with open(config_file, "w") as toml_file:
#                     config_data = toml.dump(config_data, toml_file)
#                     log(
#                         f"TOML data loaded successfully to prod: \n {config_data}"
#                     )

#             def use_config():
#                 return toml.load(config_file)["bucket_name"]

#         bucket = use_config()
#         log(f"[DEBUG] Bucket definido após config: {bucket}")


def create_credentials(config_path="/root/.basedosdados/"):
    """
    Decodifica e grava o config.toml, se a env existir.
    Não altera dinamicamente os valores (isso será feito no código principal).
    """
    log("Creating credentials and config file")
    config_path = Path(config_path)
    config_file = config_path / "config.toml"

    if os.getenv("BASEDOSDADOS_CONFIG"):
        log("Environment variable BASEDOSDADOS_CONFIG found.")
        with open(config_file, "w", encoding="utf-8") as f:
            f.write(_decode_env("BASEDOSDADOS_CONFIG"))
    else:
        log("Environment variable BASEDOSDADOS_CONFIG not found.")


@task
def template_upload_to_gcs_and_materialization(
    dataset_id: str,
    table_id: str,
    data_path: str,
    target: str,
    labels: str,
    source_format: str = "csv",
    dbt_alias: str = True,
    dump_mode: str = "append",
    run_model: str = "run/test",
    wait=None,
):
    create_credentials()

    if target == "dev":
        bucket_name = "basedosdados-dev"
        billing_project_id = "basedosdados-dev"
    elif target == "prod":
        bucket_name = "basedosdados"
        billing_project_id = "basedosdados"
    else:
        raise ValueError(f"Target inválido: {target}")

    create_table_and_upload_to_gcs_teste(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        bucket_name=bucket_name,
        billing_project_id=billing_project_id,
        source_format=source_format,
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
        f"[INFO] Upload + materialization finalizado para {dataset_id}.{table_id}"
    )
