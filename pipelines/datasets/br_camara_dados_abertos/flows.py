"""
Flows for br_camara_dados_abertos

Prefect 3 migration — original Prefect 0.x code preserved below as comments.
"""

import os
from pathlib import Path

import basedosdados as bd
import requests
from prefect import flow, task

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

DATASET_ID = "br_camara_dados_abertos"
TABLE_ID = "deputado"

URL = "https://dadosabertos.camara.leg.br/arquivos/deputados/csv/deputados.csv"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.3"
    )
}
OUTPUT_DIR = "/tmp/output/deputado"
OUTPUT_FILE = f"{OUTPUT_DIR}/deputados.csv"


# ──────────────────────────────────────────────────────────────────────────────
# Tasks
# ──────────────────────────────────────────────────────────────────────────────


@task(retries=3, retry_delay_seconds=10)
def check_url() -> bool:
    response = requests.get(URL, headers=HEADERS, timeout=30)
    if response.status_code == 200:
        print(f"URL disponível: {URL}")
        return True
    raise ValueError(f"URL retornou status {response.status_code}: {URL}")


@task(retries=3, retry_delay_seconds=10)
def download_deputado() -> str:
    import csv
    import io

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    response = requests.get(URL, headers=HEADERS, timeout=60)
    response.raise_for_status()

    # O CSV da Câmara usa ";" como separador — converte para "," antes de subir
    content = response.content.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(content), delimiter=";")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerows(reader)

    print(f"Download concluído → {OUTPUT_FILE}")
    return OUTPUT_DIR


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
    print(
        f"Upload dev concluído: gs://basedosdados-dev/staging/{dataset_id}/{table_id}"
    )


@task
def upload_to_gcs_prod(data_path: str, dataset_id: str, table_id: str) -> None:
    st = bd.Storage(
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
    )
    st.upload(path=data_path, mode="staging", if_exists="replace")
    print(
        f"Upload prod concluído: gs://basedosdados/staging/{dataset_id}/{table_id}"
    )


@task
def run_dbt_model(
    dataset_id: str, table_id: str, target: str = "dev", dbt_alias: bool = True
) -> None:
    from dbt.cli.main import dbtRunner

    model = f"{dataset_id}__{table_id}" if dbt_alias else table_id
    model_path = Path("models") / dataset_id / f"{model}.sql"

    if not model_path.exists():
        raise FileNotFoundError(f"Modelo dbt não encontrado: {model_path}")

    runner = dbtRunner()
    for command in ["run", "test"]:
        result = runner.invoke(
            [command, "--select", model, "--target", target]
        )
        if result.exception:
            raise Exception(
                f"dbt {command} exception para {model} (target={target}): {result.exception}"
            )
        if not result.success:
            logs = []
            for event in result.result or []:
                msg = getattr(event, "message", None) or str(event)
                logs.append(msg)
                print(f"dbt | {msg}")
            raise Exception(
                f"dbt {command} falhou para {model} (target={target})"
            )
        print(f"dbt {command} concluído: {model} (target={target})")


# ──────────────────────────────────────────────────────────────────────────────
# Flow
# ──────────────────────────────────────────────────────────────────────────────


@flow(log_prints=True)
def br_camara_dados_abertos__deputado(
    dataset_id: str = DATASET_ID,
    table_id: str = TABLE_ID,
    materialize_after_dump: bool = True,
    upload_to_prod: bool = False,
    dbt_alias: bool = True,
):
    url_ok = check_url()
    if not url_ok:
        return

    data_path = download_deputado()

    upload_to_gcs_dev(
        data_path=data_path, dataset_id=dataset_id, table_id=table_id
    )

    if materialize_after_dump:
        run_dbt_model(
            dataset_id=dataset_id,
            table_id=table_id,
            target="dev",
            dbt_alias=dbt_alias,
        )

    if upload_to_prod:
        upload_to_gcs_prod(
            data_path=data_path, dataset_id=dataset_id, table_id=table_id
        )
        run_dbt_model(
            dataset_id=dataset_id,
            table_id=table_id,
            target="prod",
            dbt_alias=dbt_alias,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Schedules
# ──────────────────────────────────────────────────────────────────────────────

br_camara_dados_abertos__deputado.deploy_schedules = []

# ──────────────────────────────────────────────────────────────────────────────
# Prefect 0.x — código original preservado para referência
# ──────────────────────────────────────────────────────────────────────────────

# from copy import deepcopy
# from prefect.run_configs import KubernetesRun
# from prefect.storage import GCS
# from pipelines.constants import constants
# from pipelines.crawler.camara_dados_abertos.flows import flow_camara_dados_abertos
# from pipelines.datasets.br_camara_dados_abertos.schedules import (
#     schedules_br_camara_dados_abertos_deputado, ...
# )
#
# br_camara_dados_abertos__deputado = deepcopy(flow_camara_dados_abertos)
# br_camara_dados_abertos__deputado.name = "br_camara_dados_abertos.deputado"
# br_camara_dados_abertos__deputado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# br_camara_dados_abertos__deputado.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_camara_dados_abertos__deputado.schedule = schedules_br_camara_dados_abertos_deputado
