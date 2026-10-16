"""
Flows de br_inep_censo_educacao_superior - Prefect 3.

Pipeline em estágios: check_update -> extract_and_load -> build_and_promote, um
par de flows por tabela (`curso` e `ies`). A lógica mora em `tasks.py` e
`utils.py`; aqui fica só a ligação com o `CheckThenExtractLoadPipeline`.

O check_update roda uma vez por mês. O Inep publica uma edição por ano, mas
também republica edições antigas, às vezes com correções; ler o `Last-Modified`
de todos os anos custa uma requisição de cabeçalho por ano.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_inep_censo_educacao_superior.tasks import (
    DATASET_ID,
    make_pipeline,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ---------------------------------------------------------------------- curso

_curso_pipeline = make_pipeline("curso")


@flow(name=_curso_pipeline.check_update_flow_name, log_prints=True)
def br_inep_censo_educacao_superior_curso_check_update() -> None:
    _curso_pipeline.run_check_update()


br_inep_censo_educacao_superior_curso_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "curso"
)
br_inep_censo_educacao_superior_curso_check_update.deploy_schedules = [
    Cron("27 10 15 * *", timezone="America/Sao_Paulo")
]


@flow(name=_curso_pipeline.extract_and_load_flow_name, log_prints=True)
def br_inep_censo_educacao_superior_curso_download(
    download_params: dict,
) -> None:
    _curso_pipeline.run_extract_and_load(download_params)


br_inep_censo_educacao_superior_curso_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "curso"
)
_curso_pipeline.extract_load_deployment = (
    br_inep_censo_educacao_superior_curso_download.fn.__name__
)

# ------------------------------------------------------------------------ ies

_ies_pipeline = make_pipeline("ies")


@flow(name=_ies_pipeline.check_update_flow_name, log_prints=True)
def br_inep_censo_educacao_superior_ies_check_update() -> None:
    _ies_pipeline.run_check_update()


br_inep_censo_educacao_superior_ies_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "ies"
)
br_inep_censo_educacao_superior_ies_check_update.deploy_schedules = [
    Cron("32 10 15 * *", timezone="America/Sao_Paulo")
]


@flow(name=_ies_pipeline.extract_and_load_flow_name, log_prints=True)
def br_inep_censo_educacao_superior_ies_download(
    download_params: dict,
) -> None:
    _ies_pipeline.run_extract_and_load(download_params)


br_inep_censo_educacao_superior_ies_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "ies"
)
_ies_pipeline.extract_load_deployment = (
    br_inep_censo_educacao_superior_ies_download.fn.__name__
)
