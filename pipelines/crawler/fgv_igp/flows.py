"""
Flow compartilhado para br_fgv_igp — Prefect 3.
"""

from pipelines.crawler.fgv_igp.tasks import clean_fgv_df, crawler_fgv
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _run_fgv_igp(
    dataset_id: str,
    table_id: str,
    indice: str,
    periodo: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    target: str,
    force_run: bool = False,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    # pyrefly: ignore [no-matching-overload]
    df_indice = crawler_fgv(indice, periodo)
    filepath = clean_fgv_df(df_indice)

    upload_to_gcs(
        data_path=filepath,
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
        data_path=filepath,
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
