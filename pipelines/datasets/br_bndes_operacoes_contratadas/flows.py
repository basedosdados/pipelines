"""
Flows para br_bndes_operacoes_contratadas — Prefect 3.

5 das 6 tabelas migradas pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

A lógica de download/clean (`get_source_last_modified`, `download_csv`,
`clean*`) segue existindo em `pipelines/crawler/bndes/` — reaproveitada por
`tasks.py`, não reescrita.

A exceção é `operacoes_pre_embarque`, que segue o desenho de diretório
único e ainda não foi migrada pro pipeline em estágios: o corpo fica no
próprio flow, e o download/limpeza, em `constants`/`utils`/`tasks` deste
mesmo diretório.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_bndes_operacoes_contratadas.constants import (
    DATASET_ID,
    OPERACOES_ADMINISTRACAO_PUBLICA_TABLE_ID,
    OPERACOES_EXPORTACAO_BENS_TABLE_ID,
    OPERACOES_EXPORTACAO_SERVICOS_TABLE_ID,
    OPERACOES_INDIRETAS_AUTOMATICAS_TABLE_ID,
    OPERACOES_NAO_AUTOMATICAS_TABLE_ID,
    SOURCE_DATE_FORMAT,
)
from pipelines.datasets.br_bndes_operacoes_contratadas.tasks import (
    clean_table,
    download_table,
    extract_load_data_administracao_publica,
    extract_load_data_exportacao_bens,
    extract_load_data_exportacao_servicos,
    get_latest_update_administracao_publica,
    get_latest_update_exportacao_bens,
    get_latest_update_exportacao_servicos,
    get_source_last_modified,
    make_extract_load_data,
    make_get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

# ──────────────────────────────────────────────────────────────────────────────
# operacoes_indiretas_automaticas
# check_update: br_bndes_operacoes_contratadas__operacoes_indiretas_automaticas
# extract_and_load: br_bndes_operacoes_contratadas__operacoes_indiretas_automaticas
# ──────────────────────────────────────────────────────────────────────────────

_operacoes_indiretas_automaticas_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=OPERACOES_INDIRETAS_AUTOMATICAS_TABLE_ID,
    get_latest_update=make_get_latest_update(
        OPERACOES_INDIRETAS_AUTOMATICAS_TABLE_ID
    ),
    extract_load_data=make_extract_load_data(
        OPERACOES_INDIRETAS_AUTOMATICAS_TABLE_ID
    ),
    date_format=SOURCE_DATE_FORMAT,
)


@flow(
    name=_operacoes_indiretas_automaticas_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_check_update() -> (
    None
):
    _operacoes_indiretas_automaticas_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, OPERACOES_INDIRETAS_AUTOMATICAS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (crawler/bndes).
br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_check_update.deploy_schedules = [
    Cron("0 6 * * 1", timezone="America/Sao_Paulo")
]


@flow(
    name=_operacoes_indiretas_automaticas_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_download(
    download_params: dict,
) -> None:
    _operacoes_indiretas_automaticas_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    OPERACOES_INDIRETAS_AUTOMATICAS_TABLE_ID,
)
_operacoes_indiretas_automaticas_pipeline.extract_load_deployment = br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# operacoes_nao_automaticas
# check_update: br_bndes_operacoes_contratadas__operacoes_nao_automaticas
# extract_and_load: br_bndes_operacoes_contratadas__operacoes_nao_automaticas
# ──────────────────────────────────────────────────────────────────────────────

_operacoes_nao_automaticas_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=OPERACOES_NAO_AUTOMATICAS_TABLE_ID,
    get_latest_update=make_get_latest_update(
        OPERACOES_NAO_AUTOMATICAS_TABLE_ID
    ),
    extract_load_data=make_extract_load_data(
        OPERACOES_NAO_AUTOMATICAS_TABLE_ID
    ),
    date_format=SOURCE_DATE_FORMAT,
)


@flow(
    name=_operacoes_nao_automaticas_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_nao_automaticas_check_update() -> (
    None
):
    _operacoes_nao_automaticas_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_nao_automaticas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, OPERACOES_NAO_AUTOMATICAS_TABLE_ID
)
br_bndes_operacoes_contratadas_operacoes_nao_automaticas_check_update.deploy_schedules = [
    Cron("0 6 * * 1", timezone="America/Sao_Paulo")
]


@flow(
    name=_operacoes_nao_automaticas_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_nao_automaticas_download(
    download_params: dict,
) -> None:
    _operacoes_nao_automaticas_pipeline.run_extract_and_load(download_params)


br_bndes_operacoes_contratadas_operacoes_nao_automaticas_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, OPERACOES_NAO_AUTOMATICAS_TABLE_ID
)
_operacoes_nao_automaticas_pipeline.extract_load_deployment = br_bndes_operacoes_contratadas_operacoes_nao_automaticas_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# operacoes_administracao_publica
# check_update: br_bndes_operacoes_contratadas__operacoes_administracao_publica
# extract_and_load: br_bndes_operacoes_contratadas__operacoes_administracao_publica
# ──────────────────────────────────────────────────────────────────────────────

_operacoes_administracao_publica_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=OPERACOES_ADMINISTRACAO_PUBLICA_TABLE_ID,
    get_latest_update=get_latest_update_administracao_publica,
    extract_load_data=extract_load_data_administracao_publica,
    date_format=SOURCE_DATE_FORMAT,
)


@flow(
    name=_operacoes_administracao_publica_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_administracao_publica_check_update() -> (
    None
):
    _operacoes_administracao_publica_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_administracao_publica_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, OPERACOES_ADMINISTRACAO_PUBLICA_TABLE_ID
)
# cron semanal (segunda 06h BRT), igual as irmas; a fonte atualiza mensal e o
# poll no-opa quando nao ha novidade.
br_bndes_operacoes_contratadas_operacoes_administracao_publica_check_update.deploy_schedules = [
    Cron("0 6 * * 1", timezone="America/Sao_Paulo")
]


@flow(
    name=_operacoes_administracao_publica_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_administracao_publica_download(
    download_params: dict,
) -> None:
    _operacoes_administracao_publica_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_operacoes_contratadas_operacoes_administracao_publica_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    OPERACOES_ADMINISTRACAO_PUBLICA_TABLE_ID,
)
_operacoes_administracao_publica_pipeline.extract_load_deployment = br_bndes_operacoes_contratadas_operacoes_administracao_publica_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# operacoes_exportacao_bens
# check_update: br_bndes_operacoes_contratadas__operacoes_exportacao_bens
# extract_and_load: br_bndes_operacoes_contratadas__operacoes_exportacao_bens
# ──────────────────────────────────────────────────────────────────────────────

_operacoes_exportacao_bens_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=OPERACOES_EXPORTACAO_BENS_TABLE_ID,
    get_latest_update=get_latest_update_exportacao_bens,
    extract_load_data=extract_load_data_exportacao_bens,
    date_format=SOURCE_DATE_FORMAT,
)


@flow(
    name=_operacoes_exportacao_bens_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_exportacao_bens_check_update() -> (
    None
):
    _operacoes_exportacao_bens_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_exportacao_bens_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, OPERACOES_EXPORTACAO_BENS_TABLE_ID
)
br_bndes_operacoes_contratadas_operacoes_exportacao_bens_check_update.deploy_schedules = [
    Cron("0 6 * * 1", timezone="America/Sao_Paulo")
]


@flow(
    name=_operacoes_exportacao_bens_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_exportacao_bens_download(
    download_params: dict,
) -> None:
    _operacoes_exportacao_bens_pipeline.run_extract_and_load(download_params)


br_bndes_operacoes_contratadas_operacoes_exportacao_bens_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, OPERACOES_EXPORTACAO_BENS_TABLE_ID
)
_operacoes_exportacao_bens_pipeline.extract_load_deployment = br_bndes_operacoes_contratadas_operacoes_exportacao_bens_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# operacoes_exportacao_servicos
# check_update: br_bndes_operacoes_contratadas__operacoes_exportacao_servicos
# extract_and_load: br_bndes_operacoes_contratadas__operacoes_exportacao_servicos
#
# Sem cron, ao contrário das irmãs: a série da fonte termina em 2015-04-28 e
# não recebe operação nova desde então. A tabela entra como carga única, por
# disparo manual do check_update (o poll segue valendo — se a fonte voltar a
# publicar, o extract_and_load dispara normalmente). Se a fonte voltar a
# publicar com regularidade, basta acrescentar `deploy_schedules` aqui.
# ──────────────────────────────────────────────────────────────────────────────

_operacoes_exportacao_servicos_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=OPERACOES_EXPORTACAO_SERVICOS_TABLE_ID,
    get_latest_update=get_latest_update_exportacao_servicos,
    extract_load_data=extract_load_data_exportacao_servicos,
    date_format=SOURCE_DATE_FORMAT,
)


@flow(
    name=_operacoes_exportacao_servicos_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_exportacao_servicos_check_update() -> (
    None
):
    _operacoes_exportacao_servicos_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_exportacao_servicos_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, OPERACOES_EXPORTACAO_SERVICOS_TABLE_ID
)


@flow(
    name=_operacoes_exportacao_servicos_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_exportacao_servicos_download(
    download_params: dict,
) -> None:
    _operacoes_exportacao_servicos_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_operacoes_contratadas_operacoes_exportacao_servicos_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, OPERACOES_EXPORTACAO_SERVICOS_TABLE_ID
)
_operacoes_exportacao_servicos_pipeline.extract_load_deployment = br_bndes_operacoes_contratadas_operacoes_exportacao_servicos_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# operacoes_pre_embarque — exceção de diretório único, ainda não migrada pro
# pipeline em estágios (ver constants.py/tasks.py/utils.py deste diretório).
# Sem cron, carga manual: a série da fonte está ativa, mas republica sem
# periodicidade declarada.
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_pre_embarque",
    log_prints=True,
)
def br_bndes_operacoes_contratadas__operacoes_pre_embarque(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_pre_embarque",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Atualiza a operacoes_pre_embarque quando o BNDES republica o recurso."""
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_last_modified()

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
            env="prod",
            date_format=DateFormat.YEAR_MD,
            compare_against="table_update",
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {table_id}!")
            return

    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=DateFormat.YEAR_MD,
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    csv_path = download_table()
    filepath = clean_table(csv_path=csv_path)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
        source_format="parquet",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="overwrite",
        source_format="parquet",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target="prod",
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllFree(
                date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
            ),
            env="prod",
            bq_project="basedosdados",
        )


br_bndes_operacoes_contratadas__operacoes_pre_embarque.deploy_schedules = [
    Cron("25 2 * * 1", timezone="America/Sao_Paulo")
]
