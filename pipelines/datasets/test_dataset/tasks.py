"""
Tasks for test_dataset. Vários pilotos convivem aqui — ver comentário no
topo de `constants.py`.
"""

import csv
from datetime import UTC, date, datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.test_dataset.constants import (
    DATASET_ID,
    EVENT_PIPELINE_PARTITIONED_TABLE_ID,
    EVENT_PIPELINE_TABLE_ID,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly
from pipelines.utils.stage_dispatch import CheckResult, DownloadResult
from pipelines.utils.tasks import upload_to_gcs

# ──────────────────────────────────────────────────────────────────────────────
# event_pipeline (issue #1867) — ver constants.py
# ──────────────────────────────────────────────────────────────────────────────


@task
def event_pipeline_write_reference_date_csv(reference_date: str) -> str:
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
    print(f"[event_pipeline_write_reference_date_csv] {path}")
    return str(path)


def event_pipeline_check_update() -> CheckResult:
    """Checagem real: usa a data de hoje como referência (sem lógica de
    fonte externa, já que este dataset é sintético — um dataset real
    faria aqui um HEAD request, listagem FTP, scraping leve, etc.)."""
    return CheckResult(reference_date=datetime.now(UTC).date())


def event_pipeline_download(download_params: dict) -> DownloadResult:
    """Download simulado (CSV pequeno com a data de referência) + upload
    pro staging. Devolve o que o `mat_test` genérico precisa."""
    reference_date = download_params["reference_date"]

    csv_path = event_pipeline_write_reference_date_csv(
        reference_date=reference_date
    )
    upload_to_gcs(
        data_path=csv_path,
        dataset_id=DATASET_ID,
        table_id=EVENT_PIPELINE_TABLE_ID,
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


# ──────────────────────────────────────────────────────────────────────────────
# event_pipeline_partitioned (issue #1867) — ver constants.py
# ──────────────────────────────────────────────────────────────────────────────


@task
def event_pipeline_partitioned_write_partitioned_csv(
    reference_date: str,
) -> str:
    """
    Cria um CSV pequeno dentro de uma estrutura de partição Hive
    (`ano=<ano>/mes=<mes>/dados.csv`) — simula um dataset real particionado
    por mês, ao contrário do `event_pipeline` acima (um arquivo só sem
    partição).

    O arquivo NÃO repete `ano`/`mes` como colunas — convenção Hive padrão
    (a mesma que o BigQuery detecta sozinho via `_is_partitioned`/
    `bd.Table.create()`): a coluna de partição só existe no caminho da
    pasta, nunca dentro do arquivo. Escrever `ano`/`mes` como colunas do
    CSV também (erro cometido na primeira versão deste arquivo) faz a
    tabela externa com particionamento Hive nativo (a cópia real, em
    `basedosdados-staging`) esperar 1 coluna por linha e achar 3 —
    `Database Error: Too many values in line`.

    Devolve o caminho do diretório BASE (não o arquivo) — `upload_to_gcs`
    espera um diretório quando os dados são particionados, pra preservar a
    estrutura `chave=valor/` como partição real no GCS (ver
    `basedosdados.Storage.upload`/`_resolve_partitions`).
    """
    ref = date.fromisoformat(reference_date)
    base = Path(f"/tmp/test_event_pipeline_partitioned/{reference_date}")
    partition_dir = base / f"ano={ref.year}" / f"mes={ref.month}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    csv_path = partition_dir / "dados.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["reference_date"])
        writer.writerow([reference_date])

    print(f"[event_pipeline_partitioned_write_partitioned_csv] {csv_path}")
    return str(base)


def event_pipeline_partitioned_check_update() -> CheckResult:
    """Mesma checagem sintética do `event_pipeline`: usa a data de hoje
    como referência."""
    return CheckResult(reference_date=datetime.now(UTC).date())


def event_pipeline_partitioned_download(
    download_params: dict,
) -> DownloadResult:
    """
    Download simulado, mas particionado por `ano=/mes=` — upload pro
    staging preserva essa estrutura (ver
    `event_pipeline_partitioned_write_partitioned_csv`). `partition_folders`
    no `DownloadResult` é o que faz o `mat_test` promover só essa fatia
    pra prod (`transfer_files_to_prod_flow`), não o staging inteiro.
    """
    reference_date = download_params["reference_date"]
    ref = date.fromisoformat(reference_date)
    partition_folder = f"ano={ref.year}/mes={ref.month}"

    base_dir = event_pipeline_partitioned_write_partitioned_csv(
        reference_date=reference_date
    )
    upload_to_gcs(
        data_path=base_dir,
        dataset_id=DATASET_ID,
        table_id=EVENT_PIPELINE_PARTITIONED_TABLE_ID,
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
        # Mesmo teste real do caminho dev->prod do event_pipeline.
        targets=["dev", "prod"],
        partition_folders=[partition_folder],
    )
