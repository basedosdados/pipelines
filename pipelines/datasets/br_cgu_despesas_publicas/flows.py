"""Flows for br_cgu_despesas_publicas — Prefect 3.

Execução mensal da despesa do Governo Federal (Portal da Transparência, CGU):
empenhado, liquidado, pago e movimentação de restos a pagar, por órgão, unidade
gestora, unidade orçamentária, função, programa, ação, plano orçamentário,
localizador e natureza da despesa, um arquivo por mês desde 2014-01. Um segundo
produto do mesmo host, ``favorecido`` (recebimentos por favorecido), é tratado
como uma segunda tabela independente — ver ``tasks.py``.

Migrado por completo pro pipeline em estágios (staged pipeline): check_update
-> extract_and_load -> build_and_promote, uma dupla de flows por tabela.
Lógica específica do dataset mora em ``tasks.py`` (reaproveitando ``utils.py``,
inalterado), constantes em ``constants.py`` — aqui só a fiação
(``CheckThenExtractLoadPipeline`` + ``@flow``).

**Why extract_and_load re-pulls a trailing window rather than rebuilding
everything.** The source restates closed months, but each month is restated on
its own schedule and the full history is ~150 files and ~10 GB — a 25-minute
download. So a scheduled run re-pulls only the last ``months_back`` months
(default 6) and uploads with ``dump_mode="append"``, which replaces exactly the
``ano=/mes=`` partitions it wrote and leaves the rest intact —
``dump_mode="overwrite"`` would rebuild the table from the window alone and
destroy the history. Restatements older than the window need a manual
``extract_and_load`` trigger with ``full_refresh=True`` in ``download_params``
(see ``tasks.py``).

O antigo ``br_cgu_despesas_publicas_flow``/``br_cgu_despesas_publicas_favorecido_flow``
monolíticos (check+download+clean+upload+dbt num só flow, por tabela) são
substituídos por completo por este módulo.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cgu_despesas_publicas.constants import (
    DATASET_ID,
    FAVORECIDO_TABLE_ID,
    TABLE_ID,
)
from pipelines.datasets.br_cgu_despesas_publicas.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# execucao
# check_update: br_cgu_despesas_publicas__execucao
# extract_and_load: br_cgu_despesas_publicas__execucao
# ──────────────────────────────────────────────────────────────────────────────

_execucao_pipeline = make_pipeline(TABLE_ID)


@flow(name=_execucao_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_despesas_publicas_execucao_check_update() -> None:
    _execucao_pipeline.run_check_update()


br_cgu_despesas_publicas_execucao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, TABLE_ID
)
# CGU refreshes the monthly execution files a few days into each month. Poll
# across a few early-month days at 06:47 BRT; the source-poll guard no-ops a
# run that finds nothing new. Mesmo cron do flow monolítico antigo (main).
br_cgu_despesas_publicas_execucao_check_update.deploy_schedules = [
    Cron("47 6 4,6,8,10 * *", timezone="America/Sao_Paulo")
]


@flow(name=_execucao_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cgu_despesas_publicas_execucao_download(download_params: dict) -> None:
    _execucao_pipeline.run_extract_and_load(download_params)


br_cgu_despesas_publicas_execucao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, TABLE_ID
)
_execucao_pipeline.extract_load_deployment = (
    br_cgu_despesas_publicas_execucao_download.fn.__name__
)
# The clean step holds one month (~60k rows x 46 string columns) at a time, but
# the raw CSVs for a six-month window are ~400 MB on disk. `memory` alone is
# silently dropped by the work pool template — the effective key is
# `memory_limit`. Mesmo valor do flow monolítico antigo.
br_cgu_despesas_publicas_execucao_download.job_variables = {
    "memory_limit": "6Gi",
    "memory_request": "2Gi",
}


# ──────────────────────────────────────────────────────────────────────────────
# favorecido — recebimentos por favorecido, a second monthly product on the
# same host. It gets its own check_update/extract_and_load pair rather than
# sharing execucao's: the two products are published independently, download
# from a different endpoint, and have no dbt cross-table dependency between
# them (no `ref()` from one table to the other in schema.yml) — just the
# accepted 1-flow-per-table trade-off, same download logic parametrized by
# `table` (see `tasks.py`/`constants.PRODUCTS`).
# check_update: br_cgu_despesas_publicas__favorecido
# extract_and_load: br_cgu_despesas_publicas__favorecido
# ──────────────────────────────────────────────────────────────────────────────

_favorecido_pipeline = make_pipeline(FAVORECIDO_TABLE_ID)


@flow(name=_favorecido_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_despesas_publicas_favorecido_check_update() -> None:
    _favorecido_pipeline.run_check_update()


br_cgu_despesas_publicas_favorecido_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, FAVORECIDO_TABLE_ID
)
# CGU regenerates the favorecidos files a few days into each month, alongside
# the execucao ones. Offset from the execucao check so the two do not compete
# for the same WAF budget. Mesmo cron do flow monolítico antigo (main).
br_cgu_despesas_publicas_favorecido_check_update.deploy_schedules = [
    Cron("58 7 4,6,8,10 * *", timezone="America/Sao_Paulo")
]


@flow(name=_favorecido_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cgu_despesas_publicas_favorecido_download(
    download_params: dict,
) -> None:
    _favorecido_pipeline.run_extract_and_load(download_params)


br_cgu_despesas_publicas_favorecido_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, FAVORECIDO_TABLE_ID
)
_favorecido_pipeline.extract_load_deployment = (
    br_cgu_despesas_publicas_favorecido_download.fn.__name__
)
# A six-month window is ~900 MB of raw CSV and ~4.5M rows held one month at a
# time. `memory` alone is silently dropped by the work pool template. Mesmo
# valor do flow monolítico antigo.
br_cgu_despesas_publicas_favorecido_download.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
