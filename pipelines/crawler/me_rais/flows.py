"""
Flow compartilhado para br_me_rais — Prefect 3.
"""

from pipelines.crawler.me_rais.tasks import (
    build_partitions,
    build_table_paths,
    crawl_rais_ftp,
    resolve_vinculos_table_id,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
    YearOnly,
)
from pipelines.utils.metadata.tasks import (
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _run_rais(
    dataset_id: str,
    table_id: str,
    year: int,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool = False,
    resolve_vinculos: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    effective_table_id = (
        resolve_vinculos_table_id(year=year, table_id=table_id)
        if resolve_vinculos
        else table_id
    )

    input_dir, output_dir = build_table_paths(effective_table_id)
    crawl_rais_ftp(year=year, table_id=effective_table_id, input_dir=input_dir)
    build_partitions(
        table_id=effective_table_id,
        year=year,
        input_dir=input_dir,
        output_dir=output_dir,
    )

    upload_to_gcs(
        data_path=output_dir,
        dataset_id=dataset_id,
        table_id=effective_table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=effective_table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=output_dir,
        dataset_id=dataset_id,
        table_id=effective_table_id,
        bucket_name="basedosdados",
        dump_mode="append",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=effective_table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=effective_table_id,
            coverage=PartBdpro(
                date_column=YearOnly(col="ano"),
                date_format=DateFormat.YEAR,
                free_lag=FreeLag(unit="years", value=1),
            ),
            env="prod",
            bq_project="basedosdados",
        )
