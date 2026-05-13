"""
Flows for br-bcb-taxa-selic

Prefect 3 migration — original Prefect 0.x code preserved below as comments.
"""

import os

import basedosdados as bd
import pandas as pd
import requests
from prefect import flow, task

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

DATASET_ID = "br_bcb_taxa_selic"
TABLE_ID = "taxa_selic"
API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json"


# ──────────────────────────────────────────────────────────────────────────────
# Tasks
# ──────────────────────────────────────────────────────────────────────────────


@task(retries=3, retry_delay_seconds=3)
def get_selic_data() -> str:
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    df = pd.DataFrame(response.json())

    os.makedirs(f"tmp/{TABLE_ID}/input", exist_ok=True)
    filepath = f"tmp/{TABLE_ID}/input/{TABLE_ID}.csv"
    df.to_csv(filepath, index=False)
    print(f"Dados salvos: {len(df)} linhas → {filepath}")
    return filepath


@task
def treat_selic_data() -> dict:
    df = pd.read_csv(f"tmp/{TABLE_ID}/input/{TABLE_ID}.csv")
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.date

    os.makedirs(f"tmp/{TABLE_ID}/output", exist_ok=True)
    filepath = f"tmp/{TABLE_ID}/output/{TABLE_ID}.csv"
    df.to_csv(filepath, index=False)
    max_date = str(df["data"].max())
    print(f"Dados tratados: {len(df)} linhas, última data: {max_date}")
    return {"save_output_path": filepath, "max_date": max_date}


@task
def upload_to_gcs_dev(data_path: str, dataset_id: str, table_id: str) -> None:
    tb = bd.Table(
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
    )
    st = bd.Storage(
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
    )

    if not tb.table_exists(mode="staging"):
        tb.create(
            path=data_path,
            if_storage_data_exists="replace",
            if_table_exists="replace",
        )

    st.upload(path=data_path, mode="staging", if_exists="replace")
    print(f"Upload concluído: gs://basedosdados-dev/staging/{dataset_id}/{table_id}")


@task
def run_dbt_model(dataset_id: str, table_id: str, dbt_alias: bool = False) -> None:
    from pathlib import Path

    from dbt.cli.main import dbtRunner

    model = f"{dataset_id}__{table_id}" if dbt_alias else table_id
    model_path = Path("models") / dataset_id / f"{model}.sql"

    if not model_path.exists():
        raise FileNotFoundError(f"Modelo dbt não encontrado: {model_path}")

    runner = dbtRunner()
    for command in ["run", "test"]:
        result = runner.invoke([command, "--select", model])
        if not result.success:
            raise Exception(f"dbt {command} falhou para {model}")
        print(f"dbt {command} concluído: {model}")


# ──────────────────────────────────────────────────────────────────────────────
# Flow
# ──────────────────────────────────────────────────────────────────────────────


@flow(log_prints=True)
def br_bcb_taxa_selic(
    dataset_id: str = DATASET_ID,
    table_id: str = TABLE_ID,
    materialize_after_dump: bool = True,
    dbt_alias: bool = False,
):
    get_selic_data()
    file_info = treat_selic_data()
    upload_to_gcs_dev(
        data_path=file_info["save_output_path"],
        dataset_id=dataset_id,
        table_id=table_id,
    )
    if materialize_after_dump:
        run_dbt_model(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_alias=dbt_alias,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Schedules
# ──────────────────────────────────────────────────────────────────────────────

br_bcb_taxa_selic.deploy_schedules = []

# ──────────────────────────────────────────────────────────────────────────────
# Prefect 0.x — código original preservado para referência
# ──────────────────────────────────────────────────────────────────────────────

# from prefect import Parameter, case
# from prefect.run_configs import KubernetesRun
# from prefect.storage import GCS
# from pipelines.constants import constants
# from pipelines.datasets.br_bcb_taxa_selic.tasks import (
#     get_data_taxa_selic,
#     treat_data_taxa_selic,
# )
# from pipelines.utils.decorators import Flow
# from pipelines.utils.metadata.tasks import update_django_metadata
# from pipelines.utils.tasks import (
#     create_table_dev_and_upload_to_gcs,
#     create_table_prod_gcs_and_run_dbt,
#     rename_current_flow_run_dataset_table,
#     run_dbt,
# )
#
# with Flow(
#     name="br_bcb_taxa_selic.taxa_selic",
#     code_owners=["lauris"],
# ) as datasets_br_bcb_taxa_selic_diaria_flow:
#     dataset_id = Parameter("dataset_id", default="br_bcb_taxa_selic", required=True)
#     table_id = Parameter("table_id", default="taxa_selic", required=True)
#     materialize_after_dump = Parameter("materialize_after_dump", default=True, required=False)
#     dbt_alias = Parameter("dbt_alias", default=False, required=False)
#     update_metadata = Parameter("update_metadata", default=False, required=False)
#
#     rename_flow_run = rename_current_flow_run_dataset_table(
#         prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id,
#     )
#     input_filepath = get_data_taxa_selic(table_id=table_id, upstream_tasks=[rename_flow_run])
#     file_info = treat_data_taxa_selic(table_id=table_id, upstream_tasks=[input_filepath])
#     wait_upload_table = create_table_dev_and_upload_to_gcs(
#         data_path=file_info["save_output_path"], dataset_id=dataset_id,
#         table_id=table_id, dump_mode="append", upstream_tasks=[file_info],
#     )
#     wait_for_materialization = run_dbt(
#         dataset_id=dataset_id, table_id=table_id,
#         dbt_command="run/test", dbt_alias=dbt_alias, upstream_tasks=[wait_upload_table],
#     )
#     with case(materialize_after_dump, True):
#         wait_upload_prod = create_table_prod_gcs_and_run_dbt(
#             data_path=file_info["save_output_path"], dataset_id=dataset_id,
#             table_id=table_id, dump_mode="append", upstream_tasks=[wait_for_materialization],
#         )
#         with case(update_metadata, True):
#             update_django_metadata(
#                 dataset_id=dataset_id, table_id=table_id,
#                 date_column_name={"date": "data"}, date_format="%Y-%m-%d",
#                 coverage_type="all_bdpro", bq_project="basedosdados",
#                 upstream_tasks=[wait_upload_prod],
#             )
#
# datasets_br_bcb_taxa_selic_diaria_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# datasets_br_bcb_taxa_selic_diaria_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
