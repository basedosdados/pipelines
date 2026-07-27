"""
Lógica compartilhada para os datasets br_cgu_* — Prefect 3.
"""

from pipelines.crawler.cgu.tasks import (
    dict_for_table,
    get_current_date_and_download_file,
    partition_data,
    read_and_partition_beneficios_cidadao,
    verify_all_url_exists_to_download,
)
from pipelines.utils.metadata.domain import (
    CoverageSpec,
    DateFormat,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _part_bdpro_year_month(year: str, month: str) -> PartBdpro:
    """Cobertura padrão dos flows CGU: part_bdpro mensal (ano/mes), free_lag 6m."""
    return PartBdpro(
        date_column=YearMonth(year=year, month=month),
        date_format=DateFormat.YEAR_MONTH,
    )


def _materialize_and_metadata(
    *,
    filepath: str,
    dataset_id: str,
    table_id: str,
    dbt_alias: bool,
    target: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    coverage: CoverageSpec,
    source_max_date=None,
) -> None:
    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
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
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
        source_format="csv",
    )
    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=coverage,
            env="prod",
            bq_project="basedosdados",
        )

        if source_max_date is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=source_max_date,
                env="prod",
                date_format="%Y-%m",
            )


def _run_cgu_cartao_pagamento(
    dataset_id: str,
    table_id: str,
    relative_month: int,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    data_source_max_date = get_current_date_and_download_file(
        table_id, dataset_id, relative_month
    )

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data:
            return

    filepath = partition_data(table_id=table_id, dataset_id=dataset_id)
    _materialize_and_metadata(
        filepath=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        target=target,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        source_max_date=data_source_max_date,
        coverage=_part_bdpro_year_month("ano_extrato", "mes_extrato"),
    )


def _run_cgu_servidores_publicos(
    dataset_id: str,
    table_id: str,
    relative_month: int,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    url_ok = verify_all_url_exists_to_download(
        dataset_id, table_id, relative_month
    )
    if not url_ok and not force_run:
        print("URLs não disponíveis; encerrando.")
        return

    data_source_max_date = get_current_date_and_download_file(
        table_id, dataset_id, relative_month
    )

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data:
            return

    filepath = partition_data(table_id=table_id, dataset_id=dataset_id)
    _materialize_and_metadata(
        filepath=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        target=target,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        source_max_date=data_source_max_date,
        coverage=_part_bdpro_year_month("ano", "mes"),
    )


def _run_cgu_licitacao_contrato(
    dataset_id: str,
    table_id: str,
    relative_month: int,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    data_source_max_date = get_current_date_and_download_file(
        table_id=table_id, dataset_id=dataset_id, relative_month=relative_month
    )

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data:
            return

    filepath = partition_data(table_id=table_id, dataset_id=dataset_id)
    _materialize_and_metadata(
        filepath=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        target=target,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        source_max_date=data_source_max_date,
        coverage=_part_bdpro_year_month("ano", "mes"),
    )


def _run_cgu_beneficios_cidadao(
    dataset_id: str,
    table_id: str,
    relative_month: int,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    data_source_max_date = get_current_date_and_download_file(
        table_id=table_id, dataset_id=dataset_id, relative_month=relative_month
    )

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data:
            return

    filepath = read_and_partition_beneficios_cidadao(table_id=table_id)
    _materialize_and_metadata(
        filepath=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        target=target,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        source_max_date=data_source_max_date,
        coverage=_part_bdpro_year_month(**dict_for_table(table_id)),
    )
