"""
Flows de br_ms_sim — Prefect 3.

Migrado pro pipeline em estágios: check_update -> extract_and_load, SEM
`build_and_promote` genérico. Motivo: 4 tabelas agregadas
(`constants.AGGREGATE_TABLES`) não têm extração própria — são puro
`dbt run/test` lendo do `microdados` recém-carregado. `dispatch_build_and_promote`
só sabe materializar uma tabela por vez, cada uma baixando seu próprio
staging de dev — as agregadas não têm staging nenhum pra baixar. Mesmo
problema/solução de `br_bcb_ifdata`/`us_sec_edgar` (download compartilhado +
ordem obrigatória no dbt): `extract_and_load` materializa tudo direto, via
`promote_primary_and_derived_tables` (`pipelines/utils/stage_dispatch.py`).

Diferença em relação ao `br_bcb_ifdata`: aqui não existe um parâmetro
`materialize_to_prod` manual — `promote_primary_and_derived_tables` já
decide sozinho se promove pra prod, pela mesma garantia estrutural de
`dispatch_build_and_promote` (`_dev_prefix()`, lê a tag `env:dev` do
deployment atual). Ninguém que testar isso em dev corre o risco de
esquecer uma flag e escrever em prod de verdade.

O `dicionario` (`models/br_ms_sim/br_ms_sim__dicionario.sql`) fica de fora
de propósito — tem ciclo de materialização próprio, não depende do
`microdados` a cada carga.
"""

from pipelines.datasets.br_ms_sim.constants import (
    AGGREGATE_COVERAGE,
    AGGREGATE_TABLES,
    COVERAGE,
    DATASET_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_ms_sim.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import DateFormat
from pipelines.utils.stage_dispatch import (
    Etapa,
    check_update_and_dispatch,
    deploy_tags,
    promote_primary_and_derived_tables,
)
from pipelines.utils.tasks import rename_flow_run_dataset_table, upload_to_gcs

_EXTRACT_AND_LOAD_DEPLOYMENT = "br_ms_sim_extract_and_load"


@flow(name=f"{Etapa.CHECK_UPDATE}: {DATASET_ID}", log_prints=True)
def br_ms_sim_check_update() -> bool:
    """Poll do ano mais recente na fonte, dispara extract_and_load."""
    rename_flow_run_dataset_table(
        prefix="Check Update: ",
        dataset_id=DATASET_ID,
        table_id=MICRODADOS_TABLE_ID,
    )

    result = get_latest_update()

    return check_update_and_dispatch(
        prefect_dataset_id=DATASET_ID,
        dataset_id=DATASET_ID,
        table_id=MICRODADOS_TABLE_ID,
        reference_date=result.reference_date,
        next_deployment=_EXTRACT_AND_LOAD_DEPLOYMENT,
        date_format=DateFormat.YEAR,
    )


br_ms_sim_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# Sem deploy_schedules: o flow monolítico antigo também não tinha cron —
# mesmo comportamento preservado (disparo manual/externo).


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def br_ms_sim_extract_and_load(download_params: dict) -> None:
    """Baixa o ano do `reference_date` e materializa microdados + agregadas.

    Args:
        download_params: recebido do check_update via `run_deployment()` —
            usa `reference_date` pra decidir o ano a baixar. Também aceita
            disparo manual direto (ex. `{"reference_date": "2015-01-01"}`)
            pra reprocessar um ano específico, no lugar do antigo parâmetro
            `ano` do flow monolítico.
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ",
        dataset_id=DATASET_ID,
        table_id=MICRODADOS_TABLE_ID,
    )

    result = extract_load_data(download_params)

    upload_to_gcs(
        data_path=result.data_path,
        dataset_id=DATASET_ID,
        table_id=MICRODADOS_TABLE_ID,
        bucket_name="basedosdados-dev",
        dump_mode=result.dump_mode,
        source_format=result.source_format,
    )

    promote_primary_and_derived_tables(
        dataset_id=DATASET_ID,
        table_id=MICRODADOS_TABLE_ID,
        data_path=result.data_path,
        coverage=COVERAGE,
        derived_table_ids=AGGREGATE_TABLES,
        derived_coverage=AGGREGATE_COVERAGE,
    )


br_ms_sim_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# `memory` não existe no template do work pool, que só conhece o par abaixo, e
# chave fora do template é descartada em silêncio — o pod ficaria no padrão.
br_ms_sim_extract_and_load.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
