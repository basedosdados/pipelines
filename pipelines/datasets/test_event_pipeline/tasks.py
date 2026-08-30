"""
Tasks for test_event_pipeline
"""

from prefect import task
from prefect.events.utilities import emit_event

from pipelines.datasets.test_event_pipeline.constants import Constants


@task
def emit_check_update_completed(
    has_new_data: bool, reference_date: str, source_url: str
) -> None:
    """
    Emits the event Automation 1 listens for, carrying the values
    check_update computed that flow_download needs downstream. When there's
    no new data, emits nothing — the automation only reacts to the event, so
    silence is enough to keep the downstream flow from firing.
    """
    if not has_new_data:
        print("Nenhum dado novo — não emite evento, downstream não dispara.")
        return
    emit_event(
        event=Constants.CHECK_UPDATE_COMPLETED_EVENT.value,
        resource={"prefect.resource.id": Constants.RESOURCE_ID.value},
        payload={"reference_date": reference_date, "source_url": source_url},
    )


@task
def simulate_download(reference_date: str, source_url: str) -> str:
    """
    Stand-in for a real download — just proves the params arrived intact.
    """
    download_path = f"/tmp/test_event_pipeline/{reference_date}.csv"
    print(
        f"[simulate_download] source_url={source_url} "
        f"reference_date={reference_date} -> {download_path}"
    )
    return download_path
