"""
Flows for test_event_pipeline — Prefect 3.

Piloto da issue #1867: prova que automações do Prefect 3 conseguem
encadear flows via evento, passando parâmetros computados em runtime
(não só parâmetros de entrada) via emit_event + Jinja no payload. Os
parâmetros trafegam como um dict serializado (JSON), não como campos fixos
— assim o conjunto de campos que o downstream precisa pode variar sem
precisar redesenhar a automação.
"""

from datetime import UTC, datetime

from prefect import flow

from pipelines.datasets.test_event_pipeline.constants import DATASET_ID
from pipelines.datasets.test_event_pipeline.tasks import (
    emit_check_update_completed,
    simulate_download,
)
from pipelines.utils.automations import deploy_tags


@flow(name="test_event_pipeline: check_update", log_prints=True)
def check_update_flow(
    has_new_data: bool = True,
    source_url: str = "https://example.com/test_event_pipeline/data.csv",
) -> None:
    """
    `has_new_data` só simula o resultado da checagem, já que este flow não
    tem uma fonte real pra consultar. A decisão de emitir ou não o evento
    fica na task, não aqui — assim outros datasets com o mesmo padrão de
    check_update não precisam repetir esse `if` em cada flow.

    `reference_date` é calculado aqui (não recebido como parâmetro de
    entrada) — é o valor que prova que a propagação via `emit_event`
    carrega dado computado em runtime, não só o que foi passado de fora.
    """
    reference_date = datetime.now(UTC).date().isoformat()
    emit_check_update_completed(
        has_new_data=has_new_data,
        download_params={
            "reference_date": reference_date,
            "source_url": source_url,
        },
    )


check_update_flow.deploy_tags = deploy_tags(DATASET_ID, "check_update")


@flow(name="test_event_pipeline: flow_download", log_prints=True)
def flow_download_flow(download_params: str) -> None:
    """
    Disparado pela Automação 1 via evento `check_update.completed`, ou
    manualmente com os parâmetros na mão (rerun/debug, sem passar pelo
    check_update) — nesse caso, montar o JSON à mão.
    """
    simulate_download(download_params=download_params)


flow_download_flow.deploy_tags = deploy_tags(DATASET_ID, "flow_download")
