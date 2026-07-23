"""
Flow compartilhado para datasets da Receita Federal (br_rf_cno, ...).
Prefect 3 — use os flows dos datasets para deploy.
"""

from pipelines.crawler.rf.tasks import (
    check_need_for_update,
    crawl,
    process_file,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    PartBdpro,
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


def _run_rf(
    dataset_id: str,
    table_id: str,
    chunksize: int,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool = False,
) -> None:
    """Lógica completa do flow Receita Federal. Chamada pelos flows de cada dataset."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    last_update_original_source = check_need_for_update(dataset_id=dataset_id)

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=last_update_original_source,
            env="prod",
            date_format="%Y-%m-%d",
        )

        if not has_new_data:
            return

    crawl(dataset_id=dataset_id, input_dir="input")

    # pyrefly: ignore [no-matching-overload]
    path = process_file(
        dataset_id=dataset_id,
        table_id=table_id,
        input_dir="input",
        output_dir="output",
        partition_date=last_update_original_source,
        chunksize=chunksize,
    )

    upload_to_gcs(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
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
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
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
            coverage=PartBdpro(
                date_column=DateOnly(col="data_extracao"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )

        if last_update_original_source is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=last_update_original_source,
                env="prod",
                date_format="%Y-%m-%d",
            )
