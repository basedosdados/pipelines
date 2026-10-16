"""
Flows br_ibge_pnadc — Prefect 3.

`microdados`: migrada pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Lógica específica em
`tasks.py` (`get_latest_update`/`extract_load_data`), constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

`dicionario`: NÃO migrada — o flow sempre reconstrói o dicionário quando
disparado, sem nenhuma checagem de "há dado novo na fonte" (a documentação do
IBGE que ele baixa não tem data de publicação exposta pra comparar; o cron
biquinzenal só redispara o build, idempotente). Como não existe conceito de
check real aqui, não faz sentido forçar esta tabela pro par
check_update/extract_and_load — ela segue como um flow só, igual já era antes
desta migração.

A tabela `microdados` referencia `dicionario` via `dictionary_model:
ref('br_ibge_pnadc__dicionario')` no teste `custom_dictionary_coverage`
(`models/br_ibge_pnadc/schema.yml`) — mas as duas NÃO compartilham download
(fontes e formatos totalmente diferentes) nem cron (trimestral vs.
biquinzenal), e já rodavam como dois flows independentes antes desta
migração. Diferente de `br_bcb_ifdata`/`au_rba_statistical_tables` (onde um
único download não decomponível gera várias tabelas que por isso precisam
materializar juntas), aqui não há atomicidade preexistente pra preservar: o
`dispatch_build_and_promote` genérico de `microdados` não introduz nenhuma
race nova — `dicionario` já é, e sempre foi, construído de forma assíncrona e
independente. Por isso `microdados` usa o `CheckThenExtractLoadPipeline`
padrão, com `dispatch_build_and_promote` normal, em vez do padrão de
materialização em lote inline.

As tabelas `educacao`/`rendimentos_outras_fontes` (`models/br_ibge_pnadc/`)
não têm flow nenhum associado — nenhuma task em `pipelines/crawler/ibge_pnadc`
ou `pipelines/datasets/br_ibge_pnadc` as referencia. Fora do escopo desta
migração.
"""

from prefect.schedules import Cron

from pipelines.crawler.ibge_pnadc.tasks import build_table_paths
from pipelines.datasets.br_ibge_pnadc.constants import (
    DATASET_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_ibge_pnadc.tasks import (
    build_dicionario_task,
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
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
# microdados
# check_update: br_ibge_pnadc__microdados
# extract_and_load: br_ibge_pnadc__microdados
# ──────────────────────────────────────────────────────────────────────────────

_microdados_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=MICRODADOS_TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def check_update() -> None:
    _microdados_pipeline.run_check_update()


check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main) — janela de publicação
# trimestral do IBGE (fevereiro/maio/agosto/novembro, dia 15 em diante).
check_update.deploy_schedules = [
    Cron("0 5 15-31 2,5,8,11 *", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def extract_and_load(download_params: dict) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)


# ──────────────────────────────────────────────────────────────────────────────
# dicionario (ainda não migrada — ver docstring do módulo)
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name="br_ibge_pnadc__dicionario",
    log_prints=True,
)
def br_ibge_pnadc__dicionario(
    dataset_id: str = "br_ibge_pnadc",
    table_id: str = "dicionario",
    materialize_after_dump: bool = False,
    target: str = "prod",
) -> None:
    """Reconstrói o dicionário da PNADC e materializa via dbt (dev e prod).

    Args:
        dataset_id: ID do dataset no BigQuery.
        table_id: Slug da tabela do dicionário.
        materialize_after_dump: Se True, sobe também para prod e materializa lá.
        target: Target dbt para a materialização em prod.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    input_dir, output_dir = build_table_paths(table_id=table_id)
    # pyrefly: ignore [no-matching-overload]
    data_path = build_dicionario_task(
        work_dir=input_dir, output_dir=output_dir
    )

    upload_to_gcs(
        data_path=data_path,
        table_id=table_id,
        dataset_id=dataset_id,
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
        data_path=data_path,
        table_id=table_id,
        dataset_id=dataset_id,
        bucket_name="basedosdados",
        dump_mode="overwrite",
        source_format="parquet",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target=target,
    )


br_ibge_pnadc__dicionario.deploy_schedules = [
    Cron("0 5 1,15 * *", timezone="America/Sao_Paulo")
]
