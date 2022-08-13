# -*- coding: utf-8 -*-
"""
Flows for br_tse_eleicoes
"""
# pylint: disable=invalid-name,line-too-long

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.datasets.br_tse_eleicoes.tasks import (
    download_before22,
    get_csv_files,
    build_candidatos,
    clean_candidatos22,
    build_bens_candidato,
    clean_bens22,
)
from pipelines.datasets.br_tse_eleicoes.schedules import (
    schedule_bens,
    schedule_candidatos,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="br_tse_eleicoes.candidatos", code_owners=["lucas_cr"]
) as br_tse_candidatos:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_tse_eleicoes", required=True)
    table_id = Parameter("table_id", default="candidatos", required=True)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    d22_task = download_before22(table_id=table_id)

    gfiles_task = get_csv_files(
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2022.zip",
        save_path="/tmp/data/",
        upstream_tasks=[d22_task],
    )

    c22_task = clean_candidatos22("/tmp/data/input", upstream_tasks=[gfiles_task])

    filepath = build_candidatos(
        "/tmp/data/raw/br_tse_eleicoes/candidatos", upstream_tasks=[c22_task]
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

br_tse_candidatos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_candidatos.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_tse_candidatos.schedule = schedule_candidatos


with Flow(
    name="br_tse_eleicoes.bens_candidato", code_owners=["lucas_cr"]
) as br_tse_bens:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_tse_eleicoes", required=True)
    table_id = Parameter("table_id", default="bens_candidato", required=True)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    d22_task = download_before22(table_id=table_id)

    gfiles_task = get_csv_files(
        url="https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_2022.zip",
        save_path="/tmp/data/",
        upstream_tasks=[d22_task],
    )

    c22_task = clean_bens22("/tmp/data/input", upstream_tasks=[gfiles_task])

    filepath = build_bens_candidato(
        "/tmp/data/raw/br_tse_eleicoes/candidatos", upstream_tasks=[c22_task]
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

br_tse_bens.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_bens.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_tse_bens.schedule = schedule_bens
