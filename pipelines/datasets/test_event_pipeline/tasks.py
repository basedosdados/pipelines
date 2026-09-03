"""
Tasks for test_event_pipeline
"""

import csv
from datetime import UTC, datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.test_event_pipeline.constants import (
    DATASET_ID,
    TABLE_ID,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly
from pipelines.utils.stage_dispatch import CheckResult, DownloadResult
from pipelines.utils.tasks import upload_to_gcs


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


def check_update() -> CheckResult:
    """Checagem real: usa a data de hoje como referência (sem lógica de
    fonte externa, já que este dataset é sintético — um dataset real
    faria aqui um HEAD request, listagem FTP, scraping leve, etc.)."""
    return CheckResult(reference_date=datetime.now(UTC).date())


def download(download_params: dict) -> DownloadResult:
    """Download simulado (CSV pequeno com a data de referência) + upload
    pro staging. Devolve o que o `mat_test` genérico precisa."""
    reference_date = download_params["reference_date"]

    csv_path = write_reference_date_csv(reference_date=reference_date)
    upload_to_gcs(
        data_path=csv_path,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name="basedosdados-dev",
        dump_mode="append",
    )

    return DownloadResult(
        coverage=AllFree(
            date_column=DateOnly(col="reference_date"),
            date_format=DateFormat.YEAR_MD,
        ).model_dump(),
        bq_project="basedosdados-dev",
        prefect_mode="dev",
        # Teste real do caminho dev->prod (issue #1867): mat_test_flow
        # roda no pool basedosdados (prod), então "prod" aqui exercita
        # transfer_files_to_prod_flow de verdade.
        targets=["dev", "prod"],
    )
