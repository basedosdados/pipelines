"""Tasks for br_rf_cno.

Contains both the low-level Receita Federal CNO crawler tasks
(``check_need_for_update``, ``list_files``, ``process_file``, ``crawl``) and
the table_id-parametrized factory functions (``make_get_latest_update``/
``make_extract_load_data``) that wire them into ``pipeline_factory``
(``pipelines.utils.stage_dispatch``) — one ``CheckThenExtractLoadPipeline``
per table (microdados, vinculos, areas, cnaes). Originally split across
``pipelines.shared.rf`` (the shared crawler tasks, below) and this module
(the dataset-specific wrappers, below that); merged here since ``br_rf_cno``
is the crawler's only consumer.
"""

import asyncio
import os
import shutil
import time
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

import httpx
import pandas as pd
from prefect import task
from tqdm import tqdm

from pipelines.constants import constants
from pipelines.datasets.br_rf_cno.constants import (
    CHUNKSIZE,
    COVERAGE,
    DATASET_ID,
)
from pipelines.datasets.br_rf_cno.constants import (
    constants as br_rf_constants,
)
from pipelines.datasets.br_rf_cno.utils import (
    download_file_async,
    process_chunk,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)
from pipelines.utils.utils import brasil_proxy_url, log


@task
def check_need_for_update(dataset_id: str, url: str | None = None) -> date:
    """
    Checks the need for an update by reading the source file's ``Last-Modified``
    HTTP header (via a HEAD request).

    A fonte (WebDAV ownCloud da Receita Federal) passou a ficar atrás de um WAF
    (F5 BIG-IP) que rejeita o método PROPFIND com ``HTTP 200`` + página HTML
    "Request Rejected". Por isso a data de referência agora é lida do header
    ``Last-Modified`` do próprio ``cno.zip`` (cujo GET/HEAD não é bloqueado),
    em vez da listagem via PROPFIND.

    Args:
        dataset_id (str): RF specific dataset name.

    Returns:
        datetime.date: The source file's last-modified date.

    Raises:
        httpx.HTTPError: If the request keeps failing after exhausting retries.
        ValueError: If the ``Last-Modified`` header is missing/unparseable.

    Notes:
        - Implementa retries com backoff exponencial para falhas transitórias
          (conexão e status 5xx).
    """
    log(f"---- Checking most recent update date for {dataset_id}")
    retries = 5
    delay = 2

    if url is None:
        url = br_rf_constants.URLS.value["url_download"]

    for attempt in range(retries):
        try:
            response = httpx.head(
                url,
                headers=br_rf_constants.HEADERS.value,
                timeout=30,
                follow_redirects=True,
                proxy=brasil_proxy_url(),
            )
            response.raise_for_status()
            break
        except httpx.HTTPError as e:
            log(f"Request attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                raise

    # pyrefly: ignore [unbound-name]
    most_recent_date = response.headers.get("Last-Modified")

    if not most_recent_date:
        raise ValueError(
            f"Could not read Last-Modified from {url}. "
            "Check if the source or its response headers have changed."
        )

    parsed_most_recent_date = datetime.strptime(
        most_recent_date, "%a, %d %b %Y %H:%M:%S GMT"
    ).date()

    log(f"Most recent update date: {parsed_most_recent_date}")
    return parsed_most_recent_date


@task
def list_files(input_dir: str) -> list:
    """
    Lists all CSV files present in the input directory.

    Args:
        input_dir (str): Directory where input CSVs are stored.

    Returns:
        list: List of filenames (only names, no paths).
    """
    log(f"---- Listing CSV files in {input_dir}")
    paths = [fp.name for fp in Path(input_dir).glob("*.csv")]
    paths_string = "\n".join(paths)
    log(f"Found {len(paths)} CSV files:\n{paths_string}")
    return paths


@task
def process_file(
    dataset_id: str,
    table_id: str,
    input_dir: str,
    output_dir: str,
    partition_date: str,
    file: str | None = None,
    chunksize: int = 1000000,
) -> str | None:
    """
    Processes and converts a CSV file to Parquet format in chunks.
    Applies renaming of tables and columns as specified.

    Args:
        dataset_id (str): RF specific dataset name.
        input_dir (str): Directory containing input CSVs.
        output_dir (str): Directory where Parquet files will be saved.
        partition_date (str): Partition date (YYYY-MM-DD).
        file (str): Filename (must exist in input_dir).
        chunksize (int): Number of rows per chunk when reading CSV.

    Returns:
        None
    """

    if file is None:
        file = br_rf_constants.TABLES_RENAME.value[dataset_id][table_id]
    try:
        # pyrefly: ignore [bad-assignment]
        partition_date = datetime.strptime(
            # pyrefly: ignore [unnecessary-type-conversion]
            str(partition_date),
            "%Y-%m-%d",
        ).date()
        log(f"Partition date {partition_date}.")
    except ValueError:
        log("Invalid partition_date format. Using raw value.")

    table_rename = br_rf_constants.TABLES_RENAME.value[dataset_id]
    filename = Path(file).name
    log(f"Processing file: {file}")

    if filename.endswith(".csv") and filename in table_rename.values():
        filepath = os.path.join(dataset_id, input_dir, filename)
        output_path = os.path.join(dataset_id, output_dir, table_id)
        os.makedirs(output_path, exist_ok=True)

        try:
            log(f"---- Processing file {filename} as table {table_id}")

            # N Chunks
            with open(filepath, encoding="latin-1") as fp:
                total_rows = sum(1 for _ in fp) - 1  # -1 header
                total_chunks = (total_rows // chunksize) + 1

            with tqdm(
                total=total_chunks, desc=f"{table_id}", unit="chunk"
            ) as pbar:
                for i, chunk in enumerate(
                    pd.read_csv(
                        filepath,
                        dtype=str,
                        encoding="latin-1",
                        sep=",",
                        chunksize=chunksize,
                    )
                ):
                    process_chunk(
                        dataset_id,
                        chunk,
                        i,
                        output_dir,
                        partition_date,
                        table_id,
                    )
                    pbar.update(1)

            os.remove(filepath)
            log(f"Removed temporary CSV {filename}")
            return output_path
        except Exception as e:
            log(f"Unable to process file: {e}", "error")
    else:
        log(f"File {file} not recognized in TABLES_RENAME, skipped.")


@task(
    retries=2,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def crawl(dataset_id: str, input_dir: str, url: str | None = None) -> None:
    """
    Downloads and unpacks the 'cno.zip' file from the given URL.

    Args:
        dataset_id (str): RF specific dataset name.
        input_dir (str): Input directory to save and unpack the file.
        url (str): URL of the ZIP file.

    Returns:
        None
    """
    if url is None:
        url = br_rf_constants.URLS.value["url_download"]
    log(f"---- Downloading CNO file from {url}")
    os.makedirs(dataset_id, exist_ok=True)

    asyncio.run(download_file_async(f"{dataset_id}/{input_dir}", f"{url}"))

    filepath = f"{dataset_id}/{input_dir}/data.zip"
    log(f"---- Unzipping files from {filepath}")
    shutil.unpack_archive(filepath, extract_dir=f"{dataset_id}/{input_dir}")
    os.remove(filepath)
    log("Download and unpack completed successfully")


# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas — ver constants.py.
#
# `make_get_latest_update`/`make_extract_load_data` são fábricas
# parametrizadas por `table_id` (mesmo shape de `br_ibge_ipca`/`tasks.py`).
# Cada tabela roda check_update/extract_and_load de forma independente: o
# `extract_load_data` de cada uma baixa+extrai o `cno.zip` (~306 MB) de novo
# via `crawl` e processa só o próprio CSV via `process_file` — redundância
# aceita (ver banner de constants.py), já que `crawl`/`process_file`
# (definidas acima neste módulo) não mudam: o zip inteiro é baixado de
# qualquer forma, só o recorte por tabela acontece depois, no
# `process_file`.
#
# `get_latest_update` não varia por tabela (mesmo zip, mesmo header
# `Last-Modified`) — `make_get_latest_update` recebe `table_id` só porque é
# o shape que `pipeline_factory` espera; o parâmetro fica sem uso ali.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        """Read the source zip's ``Last-Modified`` header (cheap, no download).

        Reuses `check_need_for_update` (definida acima neste módulo),
        unchanged from the old per-table flow (`_run_rf`, removido — era o
        flow monolítico em `pipelines/shared/rf/flows.py`, substituído por
        este `pipeline_factory`): a `HEAD` on `cno.zip`, not a
        date derived from the file's contents — see README's *Acesso à
        fonte e o WAF*.

        Returns:
            `SourceInspection` with the resolved date as `reference_date`.
            No `extra_download_params`: the download URL is a fixed
            constant (`constants.URLS`, em `constants.py`), not
            resolved per run, so there's nothing to round-trip through
            `extract_load_data`.
        """
        reference_date = check_need_for_update(dataset_id=DATASET_ID)
        return SourceInspection(reference_date=reference_date)

    return get_latest_update


def make_extract_load_data(
    table_id: str,
) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Download `cno.zip` again, process only this table's CSV.

        The source has no way to fetch a single table's slice — every
        table's `extract_and_load` downloads+extracts the whole zip on its
        own (see the banner above), then processes just its own file.

        Args:
            download_params: `{"reference_date": "<data_extracao>"}` —
                `reference_date` is `get_latest_update`'s resolved date,
                round-tripped through `check_update_and_dispatch`
                (`date.isoformat()`), as a plain `"YYYY-MM-DD"` string (no
                time component — `process_file`/`process_chunk` write the
                partition folder as `data=<date>`, and a datetime instead
                of a date there made `SAFE_CAST(data AS DATE)` return NULL
                in the incremental dbt models, so the filter never
                inserted; see README's *Causa raiz do congelamento*,
                2026-07-17 fix).

        Returns:
            `ExtractAndLoad` for `table_id`, with `dump_mode="append"`/
            `source_format="parquet"` — same as the old `_run_rf` (each run
            appends a full `data_extracao` snapshot; the incremental dbt
            models accumulate history, the staging dump itself doesn't need
            to retain it).

        Raises:
            RuntimeError: `process_file` didn't produce an output (file not
                recognized in `TABLES_RENAME`, or a processing failure —
                see logs).
        """
        reference_date = download_params["reference_date"]

        crawl(dataset_id=DATASET_ID, input_dir="input")

        output_path = process_file(
            dataset_id=DATASET_ID,
            table_id=table_id,
            input_dir="input",
            output_dir="output",
            partition_date=reference_date,
            chunksize=CHUNKSIZE,
        )
        if output_path is None:
            raise RuntimeError(
                f"process_file não gerou saída para a tabela {table_id!r} "
                f"de {DATASET_ID} — arquivo não reconhecido em "
                "TABLES_RENAME ou falha durante o processamento (ver logs)."
            )

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=output_path,
            dump_mode="append",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)
