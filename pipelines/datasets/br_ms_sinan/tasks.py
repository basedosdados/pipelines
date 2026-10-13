"""
Tasks for br_ms_sinan.
"""

import datetime

from pipelines.crawler.datasus.tasks import (
    access_ftp_download_files_async,
    decompress_dbc,
    get_last_modified_date_in_sinan_tablen,
    list_datasus_table_without_date,
    read_dbf_save_parquet_chunks,
)
from pipelines.datasets.br_ms_sinan.constants import (
    COVERAGE,
    DATASET_ID,
    MICRODADOS_DENGUE_TABLE_ID,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# microdados_dengue — única tabela do dataset hoje.
#
# Mesma família de fonte DATASUS/FTP que br_ms_cnes/br_ms_sia/br_ms_sih, com um
# desvio específico do SINAN: `get_last_modified_date_in_sinan_tablen` não
# lista "o próximo período a extrair" como `check_files_to_parse` — lê direto
# o last-modified (`ftp.dir`) dos arquivos "DENGBR*" no diretório PRELIM do
# FTP, sem depender da coverage atual. O grupo "DENGBR" é literal, igual o
# flow antigo (`_run_sinan`) — diferente de "DENG"
# (`DATASUS_DATABASE_TABLE["microdados_dengue"]`, usado por
# `list_datasus_table_without_date` pra achar os arquivos de verdade a
# baixar).
#
# `get_last_modified_date_in_sinan_tablen` devolve uma string "%Y-%m-%d"
# (apesar da anotação `-> datetime`, mesmo bug de tipo pré-existente em
# `crawler/datasus/tasks.py`) — convertida aqui pra `date` porque
# `check_update_and_dispatch` chama `.isoformat()` em `reference_date`.
#
# `extract_load_data` não depende de nada descoberto no check: igual o flow
# antigo, ele relista os arquivos do zero via `list_datasus_table_without_date`
# (sem filtro de data, cobre a tabela inteira) — por isso não usa
# `SourceInspection.extra_download_params`.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    source_max_date = get_last_modified_date_in_sinan_tablen(
        datasus_database="SINAN", datasus_database_table="DENGBR"
    )
    # retorna str de verdade, apesar da anotação `-> datetime` (bug pré-existente em crawler/datasus/tasks.py)
    reference_date = datetime.datetime.strptime(
        source_max_date,  # pyrefly: ignore [bad-argument-type]
        "%Y-%m-%d",
    ).date()

    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    ftp_files = list_datasus_table_without_date(
        dataset_id=DATASET_ID, table_id=MICRODADOS_DENGUE_TABLE_ID
    )
    dbc_files = access_ftp_download_files_async(
        file_list=ftp_files,
        dataset_id=DATASET_ID,
        table_id=MICRODADOS_DENGUE_TABLE_ID,
    )
    decompress_dbc(file_list=dbc_files, dataset_id=DATASET_ID)
    files_path = read_dbf_save_parquet_chunks(
        file_list=dbc_files,
        table_id=MICRODADOS_DENGUE_TABLE_ID,
        dataset_id=DATASET_ID,
    )

    # `read_dbf_save_parquet_chunks` grava parquet, mas o flow antigo
    # (`_run_sinan`) chamava `upload_to_gcs` sem declarar `source_format`
    # (default "csv") — mesmo bug de tipo pré-existente, preservado aqui
    # (default de `ExtractAndLoad.source_format` também é "csv").
    return ExtractAndLoad(coverage=COVERAGE.model_dump(), data_path=files_path)
