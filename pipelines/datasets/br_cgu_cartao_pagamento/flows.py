"""
Flows para br_cgu_cartao_pagamento — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.cgu.flows import _run_cgu_cartao_pagamento


def _flow_factory(table_id: str, cron: str):
    @flow(
        name=f"br_cgu_cartao_pagamento__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_cgu_cartao_pagamento",
        table_id: str = table_id,
        relative_month: int = 1,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_cgu_cartao_pagamento(
            dataset_id=dataset_id,
            table_id=table_id,
            relative_month=relative_month,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_cgu_cartao_pagamento__microdados_governo_federal = _flow_factory(
    "microdados_governo_federal", "0 20 * * *"
)
br_cgu_cartao_pagamento__microdados_defesa_civil = _flow_factory(
    "microdados_defesa_civil", "30 20 * * *"
)
br_cgu_cartao_pagamento__microdados_compras_centralizadas = _flow_factory(
    "microdados_compras_centralizadas", "0 21 * * *"
)
