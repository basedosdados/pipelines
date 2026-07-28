"""
Flows for br_ms_sih — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.datasus.flows import _run_sihsus


def _sih_flow(table_id: str, cron: str):
    @flow(
        name=f"br_ms_sih__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_ms_sih",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
        year_month_to_extract: str = "",
    ) -> None:
        _run_sihsus(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
            year_month_to_extract=year_month_to_extract,
        )

    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_ms_sih__servicos_profissionais = _sih_flow(
    "servicos_profissionais", "30 3 * * *"
)
br_ms_sih__aihs_reduzidas = _sih_flow("aihs_reduzidas", "30 6 * * *")
