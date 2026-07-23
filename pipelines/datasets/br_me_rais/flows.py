"""
Flows for br_me_rais — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.me_rais.flows import _run_rais


@flow(
    name="br_me_rais__microdados_estabelecimentos",
    log_prints=True,
)
def br_me_rais__microdados_estabelecimentos(
    dataset_id: str = "br_me_rais",
    table_id: str = "microdados_estabelecimentos",
    year: int = 2023,
    materialize_after_dump: bool = False,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_rais(
        dataset_id=dataset_id,
        table_id=table_id,
        year=year,
        materialize_after_dump=materialize_after_dump,
        dbt_alias=dbt_alias,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        resolve_vinculos=False,
    )


@flow(
    name="br_me_rais__microdados_vinculos",
    log_prints=True,
)
def br_me_rais__microdados_vinculos(
    dataset_id: str = "br_me_rais",
    table_id: str = "microdados_vinculos",
    year: int = 2023,
    materialize_after_dump: bool = False,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_rais(
        dataset_id=dataset_id,
        table_id=table_id,
        year=year,
        materialize_after_dump=materialize_after_dump,
        dbt_alias=dbt_alias,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        resolve_vinculos=True,
    )


# pyrefly: ignore [missing-attribute]
br_me_rais__microdados_estabelecimentos.deploy_schedules = []
# pyrefly: ignore [missing-attribute]
br_me_rais__microdados_vinculos.deploy_schedules = []
