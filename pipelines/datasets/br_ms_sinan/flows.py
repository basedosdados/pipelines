"""
Flows for br_ms_sinan — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.datasus.flows import _run_sinan


@flow(
    name="br_ms_sinan__microdados_dengue",
    log_prints=True,
)
def br_ms_sinan__microdados_dengue(
    dataset_id: str = "br_ms_sinan",
    table_id: str = "microdados_dengue",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_sinan(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        dbt_alias=dbt_alias,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


# Sem schedule no P0 — manter sem cron no P3.
