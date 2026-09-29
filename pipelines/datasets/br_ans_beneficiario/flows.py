"""
Flows para br_ans_beneficiario — Prefect 3.

Migrado por completo pro pipeline orientado a eventos (issue #1867):
check_update -> extract_and_load -> build_and_promote. Lógica específica do dataset mora em
`tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).
"""

from prefect import flow

from pipelines.datasets.br_ans_beneficiario.constants import (
    DATASET_ID,
    INFORMACAO_CONSOLIDADA_TABLE_ID,
)
from pipelines.datasets.br_ans_beneficiario.tasks import (
    br_ans_beneficiario_download,
    br_ans_beneficiario_get_latest_update,
)
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_informacao_consolidada_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=INFORMACAO_CONSOLIDADA_TABLE_ID,
    get_latest_update=br_ans_beneficiario_get_latest_update,
    extract_load_data=br_ans_beneficiario_download,
    # Mesma granularidade do flow antigo: dado é mensal, sem dia.
    date_format="%Y-%m",
)


@flow(
    name=_informacao_consolidada_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ans_beneficiario_informacao_consolidada_check_update() -> None:
    _informacao_consolidada_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ans_beneficiario_informacao_consolidada_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
)


@flow(
    name=_informacao_consolidada_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ans_beneficiario_informacao_consolidada_download(
    download_params: dict,
) -> None:
    _informacao_consolidada_pipeline.run_extract_and_load(download_params)


# pyrefly: ignore [missing-attribute]
br_ans_beneficiario_informacao_consolidada_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# Pico medido em produção após otimizar parquet_partition (category dtype +
# del/gc.collect() por estado): ~1.78Gi. ~1.7x de margem sobre esse valor —
# mesmo tier do flow antigo, já que o download pesado (crawler_ans) continua
# acontecendo aqui.
# pyrefly: ignore [missing-attribute]
br_ans_beneficiario_informacao_consolidada_download.job_variables = {
    "memory": "3Gi"
}
_informacao_consolidada_pipeline.extract_load_deployment = (
    br_ans_beneficiario_informacao_consolidada_download.fn.__name__
)
