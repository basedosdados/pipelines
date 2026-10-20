"""Flows for br_cgu_orcamento_publico — Prefect 3.

Migrado pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Lógica específica do
dataset mora em `tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).

Orçamento e execução da despesa do Governo Federal (Portal da Transparência,
CGU): dotação inicial e autorizada, valor empenhado e valor realizado, por
órgão, unidade orçamentária, função, subfunção, programa, ação e natureza da
despesa, um exercício por arquivo desde 2014.

The portal restates closed exercises — the 2024 file gained rows and value
months after the year ended — so every run re-downloads the whole history and
replaces the table (``dump_mode="overwrite"``). The whole source is a handful of
megabytes, which is what makes a full rebuild the cheap option as well as the
correct one.

The real check (``tasks.get_latest_update``) costs a single HEAD request: the
poll compares the exercise ZIP's ``Last-Modified`` against
``Table.Update.latest`` and the check_update flow returns without dispatching
extract_and_load when nothing changed.

Deploy: ``.github/workflows/scripts/deploy_flows.py`` auto-discovers the two
flows below; the dev pool ignores the schedule, the prod pool activates it.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cgu_orcamento_publico.constants import constants
from pipelines.datasets.br_cgu_orcamento_publico.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

DATASET_ID = constants.DATASET_ID.value
TABLE_ID = constants.TABLE_ID.value

_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
)


@flow(name=_pipeline.check_update_flow_name, log_prints=True)
def check_update() -> None:
    _pipeline.run_check_update()


check_update.deploy_tags = deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
# The portal regenerates every exercise file together, currently around 05:00
# BRT. Weekly is enough for an annual table whose closed years are restated
# now and then; the poll guard no-ops a run that finds nothing new. Same cron
# as the old monolithic flow.
check_update.deploy_schedules = [
    Cron("23 6 * * 4", timezone="America/Sao_Paulo")
]


@flow(name=_pipeline.extract_and_load_flow_name, log_prints=True)
def extract_and_load(download_params: dict) -> None:
    _pipeline.run_extract_and_load(download_params)


extract_and_load.deploy_tags = deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD)
# ~290k CSV rows streamed a few thousand at a time; the 4Gi default is ample.
# `memory` alone is silently dropped by the work pool template — the effective
# key is `memory_limit`.
extract_and_load.job_variables = {
    "memory_limit": "2Gi",
    "memory_request": "1Gi",
}
