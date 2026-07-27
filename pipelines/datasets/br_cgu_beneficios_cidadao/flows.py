"""
Flows para br_cgu_beneficios_cidadao — Prefect 3.

Redeploy para propagar o fix de detecção/download no portal da CGU
(User-Agent de browser + tratamento do 202 assíncrono + URL sem barra final)
em pipelines/crawler/cgu/utils.py e pipelines/utils/utils.py.
"""

from prefect import flow

from pipelines.crawler.cgu.flows import _run_cgu_beneficios_cidadao


def _flow_factory(table_id: str, cron: str):
    @flow(
        name=f"br_cgu_beneficios_cidadao__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_cgu_beneficios_cidadao",
        table_id: str = table_id,
        relative_month: int = 1,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_cgu_beneficios_cidadao(
            dataset_id=dataset_id,
            table_id=table_id,
            relative_month=relative_month,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_cgu_beneficios_cidadao__novo_bolsa_familia = _flow_factory(
    "novo_bolsa_familia", "0 19 * * *"
)
br_cgu_beneficios_cidadao__garantia_safra = _flow_factory(
    "garantia_safra", "15 19 * * *"
)
br_cgu_beneficios_cidadao__bpc = _flow_factory("bpc", "30 19 * * *")
