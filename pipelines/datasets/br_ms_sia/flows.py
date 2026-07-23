"""
Flows for br_ms_sia — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.datasus.flows import _run_siasus


def _sia_flow(table_id: str, cron: str):
    @flow(
        name=f"br_ms_sia__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_ms_sia",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
        year_month_to_extract: str = "",
    ) -> None:
        _run_siasus(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
            year_month_to_extract=year_month_to_extract,
        )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_ms_sia__producao_ambulatorial = _sia_flow(
    "producao_ambulatorial", "0 21 * * *"
)
br_ms_sia__psicossocial = _sia_flow("psicossocial", "0 7 * * *")
