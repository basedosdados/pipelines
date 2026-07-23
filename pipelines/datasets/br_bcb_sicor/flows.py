"""
Flows para br_bcb_sicor — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.bcb.flows import _run_bcb_sicor
from pipelines.crawler.bcb.tasks import create_load_dictionary
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _sicor_flow(
    table_id: str,
    cron: str,
    dump_mode: str = "overwrite",
    source_format: str = "parquet",
    coverage_type: str = "part_bdpro",
    historical_database: bool = True,
):
    @flow(
        name=f"br_bcb_sicor__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_bcb_sicor",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
        download_all_files: bool = False,
        local_redis_execution: bool = False,
    ) -> None:
        _run_bcb_sicor(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
            dump_mode=dump_mode,
            source_format=source_format,
            coverage_type=coverage_type,
            historical_database=historical_database,
            download_all_files=download_all_files,
            local_redis_execution=local_redis_execution,
        )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_bcb_sicor__operacao = _sicor_flow(
    table_id="operacao",
    cron="5 2 * * 1-5",
    dump_mode="append",
)

br_bcb_sicor__saldo = _sicor_flow(
    table_id="saldo",
    cron="15 4 * * 1-5",
    dump_mode="append",
)

br_bcb_sicor__liberacao = _sicor_flow(
    table_id="liberacao",
    cron="25 4 * * 1-5",
)

br_bcb_sicor__recurso_publico_complemento_operacao = _sicor_flow(
    table_id="recurso_publico_complemento_operacao",
    cron="35 4 * * 1-5",
)

br_bcb_sicor__recurso_publico_cooperado = _sicor_flow(
    table_id="recurso_publico_cooperado",
    cron="45 4 * * 1-5",
)

br_bcb_sicor__recurso_publico_gleba = _sicor_flow(
    table_id="recurso_publico_gleba",
    cron="55 4 * * 1-5",
    dump_mode="append",
)

br_bcb_sicor__recurso_publico_mutuario = _sicor_flow(
    table_id="recurso_publico_mutuario",
    cron="5 5 * * 1-5",
)

br_bcb_sicor__recurso_publico_propriedade = _sicor_flow(
    table_id="recurso_publico_propriedade",
    cron="15 5 * * 1-5",
)

br_bcb_sicor__operacoes_desclassificadas = _sicor_flow(
    table_id="operacoes_desclassificadas",
    cron="25 5 * * 1-5",
)

br_bcb_sicor__empreendimento = _sicor_flow(
    table_id="empreendimento",
    cron="35 5 * * 1-5",
    source_format="csv",
    coverage_type="all_free",
    historical_database=False,
)


@flow(
    name="br_bcb_sicor__dicionario",
    log_prints=True,
)
def br_bcb_sicor__dicionario(
    dataset_id: str = "br_bcb_sicor",
    table_id: str = "dicionario",
    materialize_after_dump: bool = True,
    dbt_alias: bool = False,
    update_metadata: bool = False,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    dicionario_filepath = create_load_dictionary()

    upload_to_gcs(
        data_path=dicionario_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
        source_format="csv",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=dicionario_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="overwrite",
        source_format="csv",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )
