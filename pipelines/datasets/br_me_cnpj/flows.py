"""
Flows for br_me_cnpj — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.me_cnpj.flows import _run_me_cnpj


def _me_cnpj_flow(table_id: str, cron: str):
    @flow(
        name=f"br_me_cnpj__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_me_cnpj",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_me_cnpj(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_me_cnpj__empresas = _me_cnpj_flow(table_id="empresas", cron="0 6 * * *")
br_me_cnpj__socios = _me_cnpj_flow(table_id="socios", cron="0 7 * * *")
br_me_cnpj__simples = _me_cnpj_flow(table_id="simples", cron="0 8 * * *")
br_me_cnpj__estabelecimentos = _me_cnpj_flow(
    table_id="estabelecimentos", cron="0 9 * * *"
)
