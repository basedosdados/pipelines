"""
Tasks for test_event_pipeline
"""

import csv
from pathlib import Path

from prefect import task
from prefect.deployments import run_deployment

from pipelines.datasets.test_event_pipeline.constants import PREFECT_DATASET_ID
from pipelines.utils.stage_dispatch import deployment_name


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
def dispatch_mat_test(
    dataset_id: str,
    table_id: str,
    coverage: dict,
    env: str,
    bq_project: str,
    prefect_mode: str,
    targets: list[str],
) -> None:
    """
    Dispara o `mat_test_flow` genérico via `run_deployment()`
    (`timeout=0` — não espera terminar). `dataset_id`/`table_id`/`coverage`/
    etc. vão como parâmetros nativos (dict de verdade, não string JSON) —
    o `mat_test_flow` os recebe tipados e o Pydantic valida sozinho.
    """
    run_deployment(
        name=deployment_name(PREFECT_DATASET_ID, "mat_test"),
        parameters={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "coverage": coverage,
            "env": env,
            "bq_project": bq_project,
            "prefect_mode": prefect_mode,
            "targets": targets,
        },
        timeout=0,
        as_subflow=True,
    )
