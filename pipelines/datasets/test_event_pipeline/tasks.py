"""
Tasks for test_event_pipeline
"""

import csv
from pathlib import Path

from prefect import task
from prefect.events.utilities import emit_event

from pipelines.datasets.test_event_pipeline.constants import DATASET_ID
from pipelines.utils.automations import dataset_resource_id, encode_params, event_name


@task
def write_reference_date_csv(reference_date: str) -> str:
    """
    Cria um CSV pequeno com a data de referência — simula o download de um
    dado real, que sobe pro staging via upload_to_gcs.
    """
    path = Path(f"/tmp/test_event_pipeline/{reference_date}.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["reference_date"])
        writer.writerow([reference_date])
    print(f"[write_reference_date_csv] {path}")
    return str(path)


@task
def emit_flow_download_completed(mat_test_params: dict) -> None:
    """Emits the event Automation 2 listens for."""
    emit_event(
        event=event_name("flow_download"),
        resource={"prefect.resource.id": dataset_resource_id(DATASET_ID)},
        payload={"mat_test_params": encode_params(mat_test_params)},
    )
