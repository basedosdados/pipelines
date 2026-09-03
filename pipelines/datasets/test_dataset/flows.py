"""
Flows for test_dataset. Vários pilotos/testes convivem aqui — ver
comentário no topo de `constants.py`.
"""

from prefect import flow

from pipelines.datasets.test_dataset.constants import (
    BACKEND_ENV,
    DATASET_ID,
    EVENT_PIPELINE_JOB_VARIABLES,
    EVENT_PIPELINE_PARTITIONED_JOB_VARIABLES,
    EVENT_PIPELINE_PARTITIONED_PREFECT_DATASET_ID,
    EVENT_PIPELINE_PARTITIONED_TABLE_ID,
    EVENT_PIPELINE_PREFECT_DATASET_ID,
    EVENT_PIPELINE_TABLE_ID,
)
from pipelines.datasets.test_dataset.tasks import (
    event_pipeline_check_update,
    event_pipeline_download,
    event_pipeline_partitioned_check_update,
    event_pipeline_partitioned_download,
)
from pipelines.utils.stage_dispatch import (
    CheckThenDownloadPipeline,
    Etapa,
    deploy_tags,
)
from pipelines.utils.tasks import run_dbt

# ──────────────────────────────────────────────────────────────────────────────
# Testes de download_data_to_gcs — um flow por condição de tamanho
#
# Cada flow chama run_dbt com target="prod", que ao final dispara
# download_data_to_gcs automaticamente. Os modelos dbt usam FARM_FINGERPRINT
# para gerar dados sintéticos com tamanhos previsíveis.
#
# Caso 1 — ate 100 MB, sem bdpro_filter  → exporta open
# Caso 2 — ate 100 MB, com bdpro_filter  → exporta open + BDPro (post-hook no .sql)
# Caso 3 — 100 MB-1 GB                 → exporta apenas BDPro
# Caso 4 — acima 1 GB                      → sem export (retorna cedo)
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name="test_dataset: download_data_to_gcs (ate 100 MB sem bdpro)",
    flow_run_name="test download_data_to_gcs: open only",
    log_prints=True,
)
def test_download_data_to_gcs_open_only_flow() -> None:
    run_dbt(
        dataset_id=DATASET_ID,
        table_id="tabela_pequena",
        dbt_command="run",
        target="prod",
    )


@flow(
    name="test_dataset: download_data_to_gcs (ate 100 MB com bdpro)",
    flow_run_name="test download_data_to_gcs: open and bdpro",
    log_prints=True,
)
def test_download_data_to_gcs_open_and_bdpro_flow() -> None:
    run_dbt(
        dataset_id=DATASET_ID,
        table_id="tabela_pequena_bdpro",
        dbt_command="run",
        target="prod",
    )


@flow(
    name="test_dataset: download_data_to_gcs (100 MB-1 GB)",
    flow_run_name="test download_data_to_gcs: bdpro only",
    log_prints=True,
)
def test_download_data_to_gcs_bdpro_only_flow() -> None:
    run_dbt(
        dataset_id=DATASET_ID,
        table_id="tabela_media",
        dbt_command="run",
        target="prod",
    )


@flow(
    name="test_dataset: download_data_to_gcs (acima 1 GB)",
    flow_run_name="test download_data_to_gcs: skip large",
    log_prints=True,
)
def test_download_data_to_gcs_skip_large_flow() -> None:
    run_dbt(
        dataset_id=DATASET_ID,
        table_id="tabela_grande",
        dbt_command="run",
        target="prod",
    )


@flow(
    name="test_dataset: download_data_to_gcs (suite)",
    flow_run_name="test download_data_to_gcs: all cases",
    log_prints=True,
)
def test_download_data_to_gcs_all_cases_flow() -> None:
    test_download_data_to_gcs_open_only_flow()
    test_download_data_to_gcs_open_and_bdpro_flow()
    test_download_data_to_gcs_bdpro_only_flow()
    test_download_data_to_gcs_skip_large_flow()


# ──────────────────────────────────────────────────────────────────────────────
# event_pipeline — piloto da arquitetura orientada a eventos (issue #1867)
#
# check_update_flow compara a data de hoje contra a coverage registrada no
# backend pra test_dataset.test_event_pipeline. flow_download_flow simula
# um download (CSV pequeno) e sobe pro staging. A materialização de
# verdade (dbt run/test em dev e prod + atualização da coverage) é feita
# pelo mat_test_flow genérico (pipelines/utils/metadata/flows.py).
#
# Cadeia: check_update -> (run_deployment) -> flow_download -> (run_deployment) -> mat_test.
#
# Nomes de variável com prefixo `event_pipeline_` de propósito: esse
# módulo tem várias pipelines, e `deploy_flows.py` descobre flows pelo
# nome da variável — duas pipelines aqui não podem, as duas, se chamar
# `check_update_flow` (a segunda sobrescreveria a primeira no namespace
# do módulo). Por isso `_event_pipeline.flow_download_deployment` é
# setado logo depois de `event_pipeline_flow_download_flow` existir, a
# partir do `__name__` da própria função — não repetido como string solta
# no construtor (evita o mesmo tipo de risco que o `Etapa(StrEnum)`
# elimina: duas grafias do mesmo nome que podem divergir silenciosamente).
# ──────────────────────────────────────────────────────────────────────────────

_event_pipeline = CheckThenDownloadPipeline(
    dataset_id=DATASET_ID,
    table_id=EVENT_PIPELINE_TABLE_ID,
    prefect_dataset_id=EVENT_PIPELINE_PREFECT_DATASET_ID,
    env=BACKEND_ENV,
    check_fn=event_pipeline_check_update,
    download_fn=event_pipeline_download,
)


@flow(
    name=f"{EVENT_PIPELINE_PREFECT_DATASET_ID}: check_update", log_prints=True
)
def event_pipeline_check_update_flow() -> None:
    """Disparado pelo schedule. Todo o corpo mora em `_event_pipeline` —
    ver `CheckThenDownloadPipeline` em `pipelines/utils/stage_dispatch.py`."""
    _event_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
event_pipeline_check_update_flow.deploy_tags = deploy_tags(
    EVENT_PIPELINE_PREFECT_DATASET_ID, Etapa.CHECK_UPDATE
)
# pyrefly: ignore [missing-attribute]
event_pipeline_check_update_flow.job_variables = EVENT_PIPELINE_JOB_VARIABLES[
    Etapa.CHECK_UPDATE
]


@flow(
    name=f"{EVENT_PIPELINE_PREFECT_DATASET_ID}: flow_download", log_prints=True
)
def event_pipeline_flow_download_flow(download_params: dict) -> None:
    """
    Disparado por `event_pipeline_check_update_flow` via `run_deployment()`,
    ou manualmente (rerun/debug) passando `download_params` à mão.
    """
    _event_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
event_pipeline_flow_download_flow.deploy_tags = deploy_tags(
    EVENT_PIPELINE_PREFECT_DATASET_ID, Etapa.FLOW_DOWNLOAD
)
# pyrefly: ignore [missing-attribute]
event_pipeline_flow_download_flow.job_variables = EVENT_PIPELINE_JOB_VARIABLES[
    Etapa.FLOW_DOWNLOAD
]
_event_pipeline.flow_download_deployment = (
    event_pipeline_flow_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# event_pipeline_partitioned — variante do event_pipeline testando dados
# particionados (ano=/mes=) de ponta a ponta — o event_pipeline usa um
# único arquivo sem partição, então nunca exercitou
# DownloadResult.partition_folders nem o caminho de
# transfer_files_to_prod_flow que promove só a fatia particionada, não o
# staging inteiro.
# ──────────────────────────────────────────────────────────────────────────────

_event_pipeline_partitioned = CheckThenDownloadPipeline(
    dataset_id=DATASET_ID,
    table_id=EVENT_PIPELINE_PARTITIONED_TABLE_ID,
    prefect_dataset_id=EVENT_PIPELINE_PARTITIONED_PREFECT_DATASET_ID,
    env=BACKEND_ENV,
    check_fn=event_pipeline_partitioned_check_update,
    download_fn=event_pipeline_partitioned_download,
)


@flow(
    name=f"{EVENT_PIPELINE_PARTITIONED_PREFECT_DATASET_ID}: check_update",
    log_prints=True,
)
def event_pipeline_partitioned_check_update_flow() -> None:
    """Disparado manualmente pro teste. Todo o corpo mora em
    `_event_pipeline_partitioned`."""
    _event_pipeline_partitioned.run_check_update()


# pyrefly: ignore [missing-attribute]
event_pipeline_partitioned_check_update_flow.deploy_tags = deploy_tags(
    EVENT_PIPELINE_PARTITIONED_PREFECT_DATASET_ID, Etapa.CHECK_UPDATE
)
# pyrefly: ignore [missing-attribute]
event_pipeline_partitioned_check_update_flow.job_variables = (
    EVENT_PIPELINE_PARTITIONED_JOB_VARIABLES[Etapa.CHECK_UPDATE]
)


@flow(
    name=f"{EVENT_PIPELINE_PARTITIONED_PREFECT_DATASET_ID}: flow_download",
    log_prints=True,
)
def event_pipeline_partitioned_flow_download_flow(
    download_params: dict,
) -> None:
    """
    Disparado por `event_pipeline_partitioned_check_update_flow` via
    `run_deployment()`, ou manualmente (rerun/debug) passando
    `download_params` à mão.
    """
    _event_pipeline_partitioned.run_download(download_params)


# pyrefly: ignore [missing-attribute]
event_pipeline_partitioned_flow_download_flow.deploy_tags = deploy_tags(
    EVENT_PIPELINE_PARTITIONED_PREFECT_DATASET_ID, Etapa.FLOW_DOWNLOAD
)
# pyrefly: ignore [missing-attribute]
event_pipeline_partitioned_flow_download_flow.job_variables = (
    EVENT_PIPELINE_PARTITIONED_JOB_VARIABLES[Etapa.FLOW_DOWNLOAD]
)
_event_pipeline_partitioned.flow_download_deployment = (
    event_pipeline_partitioned_flow_download_flow.fn.__name__
)
