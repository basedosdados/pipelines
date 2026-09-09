"""
Flows para br_ans_beneficiario — Prefect 3.

Migrado por completo pro pipeline orientado a eventos (issue #1867):
check_update -> download -> mat_test. Lógica específica do dataset mora em
`tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenDownloadPipeline` + `@flow`).
"""

from prefect import flow

from pipelines.datasets.br_ans_beneficiario.constants import (
    DATASET_ID,
    INFORMACAO_CONSOLIDADA_TABLE_ID,
)
from pipelines.datasets.br_ans_beneficiario.tasks import (
    br_ans_beneficiario_check_for_update,
    br_ans_beneficiario_download,
)
from pipelines.utils.stage_dispatch import (
    CheckThenDownloadPipeline,
    Etapa,
    deploy_tags,
)

_informacao_consolidada_pipeline = CheckThenDownloadPipeline(
    dataset_id=DATASET_ID,
    table_id=INFORMACAO_CONSOLIDADA_TABLE_ID,
    check_for_update=br_ans_beneficiario_check_for_update,
    download_data=br_ans_beneficiario_download,
    # Mesma granularidade do flow antigo: dado é mensal, sem dia.
    date_format="%Y-%m",
)


@flow(
    name=_informacao_consolidada_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ans_beneficiario_informacao_consolidada_check_update_flow() -> None:
    _informacao_consolidada_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ans_beneficiario_informacao_consolidada_check_update_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
)


@flow(
    name=_informacao_consolidada_pipeline.download_flow_name,
    log_prints=True,
)
def br_ans_beneficiario_informacao_consolidada_download_flow(
    download_params: dict,
) -> None:
    _informacao_consolidada_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ans_beneficiario_informacao_consolidada_download_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.DOWNLOAD)
)
# Pico medido em produção após otimizar parquet_partition (category dtype +
# del/gc.collect() por estado): ~1.78Gi. ~1.7x de margem sobre esse valor —
# mesmo tier do flow antigo, já que o download pesado (crawler_ans) continua
# acontecendo aqui.
# pyrefly: ignore [missing-attribute]
br_ans_beneficiario_informacao_consolidada_download_flow.job_variables = {
    "memory": "3Gi"
}
_informacao_consolidada_pipeline.download_deployment = (
    br_ans_beneficiario_informacao_consolidada_download_flow.fn.__name__
)
