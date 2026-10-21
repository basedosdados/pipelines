"""
Flows para br_cvm_fi — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

O antigo `_run_cvm_fi` monolítico (chamado por um `@flow` hoje removido) segue
definido logo abaixo, preservado como código legado — não é mais referenciado
por nenhum flow ativo, mas a lógica de scraping/parsing que ele orquestrava
(`extract_links_and_dates`, `generate_links_to_download`, `download_unzip`,
`clean_cvm_data`, em `tasks.py`) continua sendo reaproveitada pelo pipeline em
estágios via `make_get_latest_update`/`make_extract_load_data`.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cvm_fi.constants import (
    DATASET_ID,
    DOCUMENTOS_BALANCETE_TABLE_ID,
    DOCUMENTOS_CARTEIRAS_FUNDOS_INVESTIMENTO_TABLE_ID,
    DOCUMENTOS_EXTRATOS_INFORMACOES_TABLE_ID,
    DOCUMENTOS_INFORMACAO_CADASTRAL_TABLE_ID,
    DOCUMENTOS_INFORME_DIARIO_TABLE_ID,
    DOCUMENTOS_PERFIL_MENSAL_TABLE_ID,
)
from pipelines.datasets.br_cvm_fi.tasks import (
    clean_cvm_data,
    download_unzip,
    extract_links_and_dates,
    generate_links_to_download,
    make_pipeline,
)
from pipelines.utils.flow import flow
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
from pipelines.utils.stage_dispatch import Etapa, deploy_tags
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

# ──────────────────────────────────────────────────────────────────────────────
# documentos_informe_diario
# check_update: br_cvm_fi__documentos_informe_diario
# extract_and_load: br_cvm_fi__documentos_informe_diario
# ──────────────────────────────────────────────────────────────────────────────

_documentos_informe_diario_pipeline = make_pipeline(
    DOCUMENTOS_INFORME_DIARIO_TABLE_ID
)


@flow(
    name=_documentos_informe_diario_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_informe_diario_check_update() -> None:
    _documentos_informe_diario_pipeline.run_check_update()


br_cvm_fi_documentos_informe_diario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DOCUMENTOS_INFORME_DIARIO_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main) — tabelas escalonadas de dez em
# dez minutos a partir das 17h, pra não disputarem slot no BigQuery juntas.
br_cvm_fi_documentos_informe_diario_check_update.deploy_schedules = [
    Cron("0 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_informe_diario_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_informe_diario_download(
    download_params: dict,
) -> None:
    _documentos_informe_diario_pipeline.run_extract_and_load(download_params)


br_cvm_fi_documentos_informe_diario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DOCUMENTOS_INFORME_DIARIO_TABLE_ID
)
_documentos_informe_diario_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_informe_diario_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# documentos_carteiras_fundos_investimento
# check_update: br_cvm_fi__documentos_carteiras_fundos_investimento
# extract_and_load: br_cvm_fi__documentos_carteiras_fundos_investimento
# ──────────────────────────────────────────────────────────────────────────────

_documentos_carteiras_fundos_investimento_pipeline = make_pipeline(
    DOCUMENTOS_CARTEIRAS_FUNDOS_INVESTIMENTO_TABLE_ID
)


@flow(
    name=_documentos_carteiras_fundos_investimento_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_carteiras_fundos_investimento_check_update() -> None:
    _documentos_carteiras_fundos_investimento_pipeline.run_check_update()


br_cvm_fi_documentos_carteiras_fundos_investimento_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID,
        Etapa.CHECK_UPDATE,
        DOCUMENTOS_CARTEIRAS_FUNDOS_INVESTIMENTO_TABLE_ID,
    )
)
# Mesmo cron do flow monolítico antigo (main).
br_cvm_fi_documentos_carteiras_fundos_investimento_check_update.deploy_schedules = [
    Cron("10 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_carteiras_fundos_investimento_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_carteiras_fundos_investimento_download(
    download_params: dict,
) -> None:
    _documentos_carteiras_fundos_investimento_pipeline.run_extract_and_load(
        download_params
    )


br_cvm_fi_documentos_carteiras_fundos_investimento_download.deploy_tags = (
    deploy_tags(
        DATASET_ID,
        Etapa.EXTRACT_AND_LOAD,
        DOCUMENTOS_CARTEIRAS_FUNDOS_INVESTIMENTO_TABLE_ID,
    )
)
_documentos_carteiras_fundos_investimento_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_carteiras_fundos_investimento_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# documentos_extratos_informacoes
# check_update: br_cvm_fi__documentos_extratos_informacoes
# extract_and_load: br_cvm_fi__documentos_extratos_informacoes
# ──────────────────────────────────────────────────────────────────────────────

_documentos_extratos_informacoes_pipeline = make_pipeline(
    DOCUMENTOS_EXTRATOS_INFORMACOES_TABLE_ID
)


@flow(
    name=_documentos_extratos_informacoes_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_extratos_informacoes_check_update() -> None:
    _documentos_extratos_informacoes_pipeline.run_check_update()


br_cvm_fi_documentos_extratos_informacoes_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID,
        Etapa.CHECK_UPDATE,
        DOCUMENTOS_EXTRATOS_INFORMACOES_TABLE_ID,
    )
)
# Mesmo cron do flow monolítico antigo (main).
br_cvm_fi_documentos_extratos_informacoes_check_update.deploy_schedules = [
    Cron("20 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_extratos_informacoes_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_extratos_informacoes_download(
    download_params: dict,
) -> None:
    _documentos_extratos_informacoes_pipeline.run_extract_and_load(
        download_params
    )


br_cvm_fi_documentos_extratos_informacoes_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    DOCUMENTOS_EXTRATOS_INFORMACOES_TABLE_ID,
)
_documentos_extratos_informacoes_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_extratos_informacoes_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# documentos_balancete
# check_update: br_cvm_fi__documentos_balancete
# extract_and_load: br_cvm_fi__documentos_balancete
# ──────────────────────────────────────────────────────────────────────────────

_documentos_balancete_pipeline = make_pipeline(DOCUMENTOS_BALANCETE_TABLE_ID)


@flow(
    name=_documentos_balancete_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_balancete_check_update() -> None:
    _documentos_balancete_pipeline.run_check_update()


br_cvm_fi_documentos_balancete_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DOCUMENTOS_BALANCETE_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cvm_fi_documentos_balancete_check_update.deploy_schedules = [
    Cron("30 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_balancete_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_balancete_download(download_params: dict) -> None:
    _documentos_balancete_pipeline.run_extract_and_load(download_params)


br_cvm_fi_documentos_balancete_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DOCUMENTOS_BALANCETE_TABLE_ID
)
_documentos_balancete_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_balancete_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# documentos_informacao_cadastral
# check_update: br_cvm_fi__documentos_informacao_cadastral
# extract_and_load: br_cvm_fi__documentos_informacao_cadastral
# ──────────────────────────────────────────────────────────────────────────────

_documentos_informacao_cadastral_pipeline = make_pipeline(
    DOCUMENTOS_INFORMACAO_CADASTRAL_TABLE_ID
)


@flow(
    name=_documentos_informacao_cadastral_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_informacao_cadastral_check_update() -> None:
    _documentos_informacao_cadastral_pipeline.run_check_update()


br_cvm_fi_documentos_informacao_cadastral_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID,
        Etapa.CHECK_UPDATE,
        DOCUMENTOS_INFORMACAO_CADASTRAL_TABLE_ID,
    )
)
# Mesmo cron do flow monolítico antigo (main).
br_cvm_fi_documentos_informacao_cadastral_check_update.deploy_schedules = [
    Cron("40 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_informacao_cadastral_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_informacao_cadastral_download(
    download_params: dict,
) -> None:
    _documentos_informacao_cadastral_pipeline.run_extract_and_load(
        download_params
    )


br_cvm_fi_documentos_informacao_cadastral_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    DOCUMENTOS_INFORMACAO_CADASTRAL_TABLE_ID,
)
_documentos_informacao_cadastral_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_informacao_cadastral_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# documentos_perfil_mensal
# check_update: br_cvm_fi__documentos_perfil_mensal
# extract_and_load: br_cvm_fi__documentos_perfil_mensal
# ──────────────────────────────────────────────────────────────────────────────

_documentos_perfil_mensal_pipeline = make_pipeline(
    DOCUMENTOS_PERFIL_MENSAL_TABLE_ID
)


@flow(
    name=_documentos_perfil_mensal_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_perfil_mensal_check_update() -> None:
    _documentos_perfil_mensal_pipeline.run_check_update()


br_cvm_fi_documentos_perfil_mensal_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DOCUMENTOS_PERFIL_MENSAL_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cvm_fi_documentos_perfil_mensal_check_update.deploy_schedules = [
    Cron("50 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_perfil_mensal_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_perfil_mensal_download(
    download_params: dict,
) -> None:
    _documentos_perfil_mensal_pipeline.run_extract_and_load(download_params)


br_cvm_fi_documentos_perfil_mensal_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DOCUMENTOS_PERFIL_MENSAL_TABLE_ID
)
_documentos_perfil_mensal_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_perfil_mensal_download.fn.__name__
)

# ──────────────────────────────────────────────────────────────────────────────
# _run_cvm_fi — legado, pré-migração pro pipeline em estágios.
# Não é mais chamado por nenhum flow ativo (o `@flow` que o invocava foi
# removido quando o dataset migrou pra `make_pipeline`, acima); mantido aqui
# só por preservação histórica da lógica de orquestração monolítica.
# ──────────────────────────────────────────────────────────────────────────────


def _run_cvm_fi(
    dataset_id: str,
    table_id: str,
    date_column_name: dict,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
    url: str | None = None,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    df, max_date = extract_links_and_dates(table_id=table_id, url=url)
    print(f"Links e datas: {df}")

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="table_update",
        )
        if not has_new_data:
            print(
                "Sem atualizações na fonte — aguardando próxima execução agendada"
            )
            return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=max_date,
        env="prod",
        date_format="%Y-%m-%d",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    # pyrefly: ignore [no-matching-overload]
    arquivos = generate_links_to_download(df=df, max_date=max_date)
    print(f"Arquivos: {arquivos}")

    input_filepath = download_unzip(table_id=table_id, files=arquivos, url=url)
    output_filepath = clean_cvm_data(
        input_dir=input_filepath, table_id=table_id, config_key=table_id
    )

    upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
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
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target=target,
    )

    if update_metadata:
        # date_column_name é parametrizado pelos flows de dataset; todos os
        # callers passam {"date": <col>} (ver br_cvm_fi), logo DateOnly/YEAR_MD.
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllBdpro(
                date_column=DateOnly(col=date_column_name["date"]),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )
