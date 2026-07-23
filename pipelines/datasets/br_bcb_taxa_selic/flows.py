"""
Flow br_bcb_taxa_selic — Prefect 3.
"""

import os

import pandas as pd
import requests
from prefect import flow, task

from pipelines.utils.metadata.domain import (
    AllBdpro,
    DateFormat,
    DateOnly,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = "br_bcb_taxa_selic"
TABLE_ID = "taxa_selic"
API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json"

# BCB rejects requests sem User-Agent (HTTP 406)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


@task(retries=3, retry_delay_seconds=10)
def get_selic_data() -> str:
    response = requests.get(API_URL, headers=_HEADERS, timeout=30)
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


@flow(
    name="br_bcb_taxa_selic__taxa_selic",
    log_prints=True,
)
def br_bcb_taxa_selic__taxa_selic(
    dataset_id: str = DATASET_ID,
    table_id: str = TABLE_ID,
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    get_selic_data()
    file_info = treat_selic_data()

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=file_info["max_date"],
            env="prod",
            date_format="%Y-%m-%d",
        )
        if not has_new_data:
            return

    upload_to_gcs(
        data_path=file_info["save_output_path"],
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
        data_path=file_info["save_output_path"],
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

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllBdpro(
                date_column=DateOnly(col="data"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )

        if file_info["max_date"] is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=file_info["max_date"],
                env="prod",
                date_format="%Y-%m-%d",
            )


br_bcb_taxa_selic__taxa_selic.deploy_schedules = [
    {"cron": "0 8 * * *", "timezone": "America/Sao_Paulo"}
]
