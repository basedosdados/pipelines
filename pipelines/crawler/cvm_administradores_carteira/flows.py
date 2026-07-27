"""
Shared run logic for br_cvm_administradores_carteira — Prefect 3.
"""

from pipelines.crawler.cvm_administradores_carteira.tasks import (
    clean_table_pessoa_fisica,
    clean_table_pessoa_juridica,
    clean_table_responsavel,
    crawl,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    PartBdpro,
)
from pipelines.utils.metadata.tasks import (
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

_CLEANERS = {
    "responsavel": clean_table_responsavel,
    "pessoa_fisica": clean_table_pessoa_fisica,
    "pessoa_juridica": clean_table_pessoa_juridica,
}


def _run_cvm_administradores_carteira(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    # source has no exposed max date — always run; force_run is a no-op here.
    _ = force_run

    crawl()
    filepath = _CLEANERS[table_id]()

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

    if update_metadata and table_id in ("pessoa_fisica", "pessoa_juridica"):
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=PartBdpro(
                date_column=DateOnly(col="data_registro"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )
