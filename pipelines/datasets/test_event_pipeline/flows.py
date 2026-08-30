"""
Flows for test_event_pipeline — Prefect 3.

Piloto da issue #1867: prova que automações do Prefect 3 conseguem
encadear flows via evento, passando parâmetros computados em runtime
(não só parâmetros de entrada) via emit_event + Jinja no payload.
"""

from prefect import flow

from pipelines.datasets.test_event_pipeline.tasks import (
    emit_check_update_completed,
    simulate_download,
)


@flow(name="test_event_pipeline: check_update", log_prints=True)
def check_update_flow(
    has_new_data: bool = True,
    reference_date: str = "2026-08-30",
    source_url: str = "https://example.com/test_event_pipeline/data.csv",
) -> None:
    """
    `has_new_data` só simula o resultado da checagem, já que este flow não
    tem uma fonte real pra consultar. A decisão de emitir ou não o evento
    fica na task, não aqui — assim outros datasets com o mesmo padrão de
    check_update não precisam repetir esse `if` em cada flow.
    """
    emit_check_update_completed(
        has_new_data=has_new_data,
        reference_date=reference_date,
        source_url=source_url,
    )


@flow(name="test_event_pipeline: flow_download", log_prints=True)
def flow_download_flow(reference_date: str, source_url: str) -> None:
    """
    Disparado pela Automação 1 via evento `check_update.completed`, ou
    manualmente com os parâmetros na mão (rerun/debug, sem passar pelo
    check_update).
    """
    simulate_download(reference_date=reference_date, source_url=source_url)
