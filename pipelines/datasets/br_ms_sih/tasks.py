"""
Tasks for br_ms_sih.
"""

from collections.abc import Callable

from pipelines.crawler.datasus.tasks import (
    access_ftp_download_files_async,
    check_files_to_parse,
    decompress_dbc,
    get_datasus_source_max_date,
    read_dbf_save_parquet_chunks,
)
from pipelines.datasets.br_ms_sih.constants import COVERAGE, DATASET_ID
from pipelines.utils.metadata.tasks import task_get_api_most_recent_date
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

# ──────────────────────────────────────────────────────────────────────────────
# As 2 tabelas — ver constants.py
#
# Mesma família de fonte DATASUS/FTP que br_ms_cnes (`check_files_to_parse`
# -> `list_datasus_dbc_files` -> `ftp.nlst(...)`, listagem leve, sem baixar
# nenhum arquivo) — único ponto que muda é o passo de conversão: SIH usa
# `read_dbf_save_parquet_chunks` (DBF -> parquet em chunks, sem
# pós-processamento por tabela), enquanto CNES usa `decompress_dbf` +
# `pre_process_files` (DBF -> CSV -> parquet com pós-processamento
# específico por tabela). Mesmo raciocínio de `extra_download_params`
# (`ftp_files`) e de fallback pra `task_get_api_most_recent_date` quando o
# FTP não acusa arquivo novo pro próximo período (`get_datasus_source_max_date`
# devolve `None`) — ver comentário equivalente em
# `pipelines/datasets/br_ms_cnes/tasks.py`.
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
