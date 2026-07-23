"""
Flows for br_ms_cnes — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.datasus.flows import _run_cnes


def _cnes_flow(table_id: str, cron: str | None):
    @flow(
        name=f"br_ms_cnes__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_ms_cnes",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
        year_month_to_extract: str = "",
    ) -> None:
        _run_cnes(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
            year_month_to_extract=year_month_to_extract,
        )

    if cron:
        # pyrefly: ignore [missing-attribute]
        _flow.deploy_schedules = [
            {"cron": cron, "timezone": "America/Sao_Paulo"}
        ]
    return _flow


br_ms_cnes__profissional = _cnes_flow("profissional", "30 6 * * *")
br_ms_cnes__estabelecimento = _cnes_flow("estabelecimento", "0 9 * * *")
br_ms_cnes__equipe = _cnes_flow("equipe", "30 9 * * *")
br_ms_cnes__leito = _cnes_flow("leito", "0 10 * * *")
br_ms_cnes__equipamento = _cnes_flow("equipamento", "30 10 * * *")
br_ms_cnes__estabelecimento_ensino = _cnes_flow("estabelecimento_ensino", None)
br_ms_cnes__dados_complementares = _cnes_flow(
    "dados_complementares", "0 11 * * *"
)
br_ms_cnes__estabelecimento_filantropico = _cnes_flow(
    "estabelecimento_filantropico", "15 11 * * *"
)
br_ms_cnes__gestao_metas = _cnes_flow("gestao_metas", "30 11 * * *")
br_ms_cnes__habilitacao = _cnes_flow("habilitacao", "45 11 * * *")
br_ms_cnes__incentivos = _cnes_flow("incentivos", "50 11 * * *")
br_ms_cnes__regra_contratual = _cnes_flow("regra_contratual", None)
br_ms_cnes__servico_especializado = _cnes_flow(
    "servico_especializado", "30 12 * * *"
)
