"""
Flows for br_fgv_igp — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.fgv_igp.flows import _run_fgv_igp


def _igp_flow(table_id: str, indice: str, periodo: str, cron: str | None):
    @flow(
        name=f"br_fgv_igp__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_fgv_igp",
        table_id: str = table_id,
        indice: str = indice,
        periodo: str = periodo,
        materialize_after_dump: bool = True,
        dbt_alias: bool = False,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_fgv_igp(
            dataset_id=dataset_id,
            table_id=table_id,
            indice=indice,
            periodo=periodo,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            target=target,
            force_run=force_run,
        )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = (
        [{"cron": cron, "timezone": "America/Sao_Paulo"}] if cron else []
    )
    return _flow


# Crons originais do P0 (commented out): mensais em dia 8, anuais em dia 8/janeiro
br_fgv_igp__igp_di_mes = _igp_flow("igp_di_mes", "IGPDI", "mes", "0 3 8 * *")
br_fgv_igp__igp_di_ano = _igp_flow("igp_di_ano", "IGPDI", "ano", "0 3 8 1 *")
br_fgv_igp__igp_m_mes = _igp_flow("igp_m_mes", "IGPM", "mes", "0 3 1 * *")
br_fgv_igp__igp_m_ano = _igp_flow("igp_m_ano", "IGPM", "ano", "0 3 1 1 *")
br_fgv_igp__igp_og_mes = _igp_flow("igp_og_mes", "IGPOG", "mes", "0 3 8 * *")
br_fgv_igp__igp_og_ano = _igp_flow("igp_og_ano", "IGPOG", "ano", "0 3 8 1 *")
br_fgv_igp__igp_10_mes = _igp_flow("igp_10_mes", "IGP10", "mes", "0 3 8 * *")
