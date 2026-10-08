"""
Tasks for br_ms_sia.
"""

from collections.abc import Callable

from pipelines.crawler.datasus.tasks import (
    access_ftp_download_files_async,
    check_files_to_parse,
    decompress_dbc,
    get_datasus_source_max_date,
    read_dbf_save_parquet_chunks,
)
from pipelines.datasets.br_ms_sia.constants import COVERAGE, DATASET_ID
from pipelines.utils.metadata.tasks import task_get_api_most_recent_date
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

# ──────────────────────────────────────────────────────────────────────────────
# As 2 tabelas — ver constants.py
#
# Mesma família de fonte que br_ms_cnes (listagem FTP DATASUS —
# `check_files_to_parse` -> `list_datasus_dbc_files` -> `ftp.nlst(...)`, sem
# baixar nenhum arquivo): get_latest_update é genuinamente independente do
# download, igual br_ms_cnes/tasks.py. A lista de arquivos FTP descobertos
# (`ftp_files`) é repassada pra `extract_load_data` via
# `SourceInspection.extra_download_params`.
#
# Diferença de br_ms_cnes: br_ms_sia não tem pós-processamento por tabela
# (sem `pre_process_files`) — o antigo `_run_dbf_to_parquet` (compartilhado
# com SIH) vai direto de .dbc pra parquet via `read_dbf_save_parquet_chunks`,
# reaproveitada aqui sem alteração.
#
# `get_datasus_source_max_date` devolve `None` quando não há arquivo novo no
# FTP pro próximo período — nesse caso caímos pra
# `task_get_api_most_recent_date` (a própria coverage atual) como
# `reference_date`, garantindo que `poll_source_for_update_task` compare data
# igual a data e conclua corretamente "sem dado novo" (mesmo efeito do
# `if not ftp_files` do flow antigo, mas sem violar o tipo de
# `SourceInspection.reference_date`, que não aceita `None`) — mesmo
# tratamento de br_ms_cnes/tasks.py.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        ftp_files = check_files_to_parse(
            dataset_id=DATASET_ID,
            table_id=table_id,
            year_month_to_extract="",
        )
        source_max_date = get_datasus_source_max_date(ftp_files)
        if source_max_date is None:
            source_max_date = task_get_api_most_recent_date(
                dataset_id=DATASET_ID,
                table_id=table_id,
                date_format="%Y-%m",
            )

        return SourceInspection(
            reference_date=source_max_date,
            extra_download_params={"ftp_files": ftp_files},
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        ftp_files = download_params["ftp_files"]

        dbc_files = access_ftp_download_files_async(
            file_list=ftp_files, dataset_id=DATASET_ID, table_id=table_id
        )
        decompress_dbc(file_list=dbc_files, dataset_id=DATASET_ID)
        files_path = read_dbf_save_parquet_chunks(
            file_list=dbc_files, table_id=table_id, dataset_id=DATASET_ID
        )

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=files_path,
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    # Mesma granularidade do flow antigo (`_run_dbf_to_parquet`, que já
    # compara coverage com date_format="%Y-%m" — arquivos DATASUS são
    # mensais).
    date_format="%Y-%m",
)
