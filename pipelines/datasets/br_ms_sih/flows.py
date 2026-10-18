"""
Flows for br_ms_sih — Prefect 3.
"""

from prefect.schedules import Cron

from pipelines.crawler.datasus.flows import _run_sihsus
from pipelines.utils.flow import flow


def _sih_flow(table_id: str, cron: str):
    @flow(
        name=f"br_ms_sih__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_ms_sih",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        update_metadata: bool = True,
        force_run: bool = False,
        year_month_to_extract: str = "",
        reprocess_months: int = 24,
        max_months_per_run: int = 6,
    ) -> None:
        """Carrega toda UF-mês nova ou reescrita no FTP do DATASUS.

        O DATASUS publica um arquivo preliminar por UF e competência, publica
        UFs atrasadas depois das demais e reescreve os arquivos por até um ano.
        Cada execução olha `reprocess_months` competências para trás e recarrega
        as UF-mês cujo arquivo no FTP é mais novo que a cópia na staging, até
        `max_months_per_run` competências por execução.

        `year_month_to_extract` ("2403 2404", "2024-03,2024-04") força a recarga
        dessas competências, substituindo as partições na staging.
        """
        _run_sihsus(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            update_metadata=update_metadata,
            force_run=force_run,
            year_month_to_extract=year_month_to_extract,
            reprocess_months=reprocess_months,
            max_months_per_run=max_months_per_run,
        )

    _flow.deploy_schedules = [Cron(cron, timezone="America/Sao_Paulo")]
    return _flow


br_ms_sih__servicos_profissionais = _sih_flow(
    "servicos_profissionais", "30 3 * * *"
)
br_ms_sih__aihs_reduzidas = _sih_flow("aihs_reduzidas", "30 6 * * *")
