"""
Flows for br_cvm_oferta_publica_distribuicao — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.cvm_oferta_publica_distribuicao.tasks import (
    clean_table_oferta_distribuicao,
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


@flow(
    name="br_cvm_oferta_publica_distribuicao__dia",
    log_prints=True,
)
def br_cvm_oferta_publica_distribuicao__dia(
    dataset_id: str = "br_cvm_oferta_publica_distribuicao",
    table_id: str = "dia",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    # Fonte sem `last_modified` exposto — sempre roda; force_run é no-op.
    _ = force_run

    crawl()
    filepath = clean_table_oferta_distribuicao()

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

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=PartBdpro(
                date_column=DateOnly(col="data_comunicado"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


br_cvm_oferta_publica_distribuicao__dia.deploy_schedules = [
    {"cron": "45 6 * * 1-5", "timezone": "America/Sao_Paulo"}
]
