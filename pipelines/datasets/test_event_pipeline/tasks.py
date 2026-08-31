"""
Tasks for test_event_pipeline
"""

from prefect import task
from prefect.events.utilities import emit_event

from pipelines.datasets.test_event_pipeline.constants import DATASET_ID
from pipelines.utils.automations import (
    dataset_resource_id,
    decode_params,
    encode_params,
    event_name,
)


@task
def emit_check_update_completed(has_new_data: bool, download_params: dict) -> None:
    """
    Emits the event Automation 1 listens for, carrying whatever fields
    flow_download needs downstream (serializados via `encode_params` — ver
    docstring lá pro motivo). When there's no new data, emits nothing — the
    automation only reacts to the event, so silence is enough to keep the
    downstream flow from firing.
    """
    if not has_new_data:
        print("Nenhum dado novo — não emite evento, downstream não dispara.")
        return
    emit_event(
        event=event_name("check_update"),
        resource={"prefect.resource.id": dataset_resource_id(DATASET_ID)},
        payload={"download_params": encode_params(download_params)},
    )


@task
def simulate_download(download_params: str) -> str:
    """
    Stand-in for a real download — just proves the params arrived intact.
    """
    params = decode_params(download_params)
    reference_date = params["reference_date"]
    source_url = params["source_url"]
    download_path = f"/tmp/test_event_pipeline/{reference_date}.csv"
    print(
        f"[simulate_download] source_url={source_url} "
        f"reference_date={reference_date} -> {download_path}"
    )
    return download_path
