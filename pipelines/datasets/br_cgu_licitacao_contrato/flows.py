"""
Flows para br_cgu_licitacao_contrato — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.cgu.flows import _run_cgu_licitacao_contrato


def _flow_factory(table_id: str, cron: str | None):
    @flow(
        name=f"br_cgu_licitacao_contrato__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_cgu_licitacao_contrato",
        table_id: str = table_id,
        relative_month: int = 1,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_cgu_licitacao_contrato(
            dataset_id=dataset_id,
            table_id=table_id,
            relative_month=relative_month,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    if cron:
        # pyrefly: ignore [missing-attribute]
        _flow.deploy_schedules = [
            {"cron": cron, "timezone": "America/Sao_Paulo"}
        ]
    return _flow


br_cgu_licitacao_contrato__contrato_compra = _flow_factory(
    "contrato_compra", "0 21 * * *"
)
br_cgu_licitacao_contrato__contrato_item = _flow_factory(
    "contrato_item", "15 20 * * *"
)
br_cgu_licitacao_contrato__contrato_termo_aditivo = _flow_factory(
    "contrato_termo_aditivo", "30 20 * * *"
)
br_cgu_licitacao_contrato__licitacao = _flow_factory(
    "licitacao", "45 20 * * *"
)
br_cgu_licitacao_contrato__licitacao_empenho = _flow_factory(
    "licitacao_empenho", None
)
br_cgu_licitacao_contrato__licitacao_item = _flow_factory(
    "licitacao_item", "20 20 * * *"
)
br_cgu_licitacao_contrato__licitacao_participante = _flow_factory(
    "licitacao_participante", "35 20 * * *"
)
