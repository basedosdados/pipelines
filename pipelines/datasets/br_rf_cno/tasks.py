"""
Tasks for br_rf_cno.
"""

from collections.abc import Callable
from datetime import date

from pipelines.crawler.rf.tasks import (
    check_need_for_update,
    crawl,
    process_file,
)
from pipelines.datasets.br_rf_cno.constants import COVERAGE, DATASET_ID
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas (microdados/vinculos/areas/cnaes) compartilham um único
# download (`crawl`, cno.zip) — cada extract_and_load refaz o crawl
# independentemente, mesmo padrão já usado em br_ms_cnes/br_ibge_ipca pra
# datasets com fonte compartilhada entre tabelas.
#
# check_need_for_update (HEAD + Last-Modified) é checagem de verdade leve e
# independente do download — não baixa o cno.zip. `process_file` grava
# parquet em `data=<partition_date>/` (convenção Hive), então
# `discover_partition_folders` acha a partição sozinho.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        reference_date = check_need_for_update(dataset_id=DATASET_ID)
        return SourceInspection(reference_date=reference_date)

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        reference_date = date.fromisoformat(download_params["reference_date"])

        crawl(dataset_id=DATASET_ID, input_dir="input")
        # pyrefly: ignore [no-matching-overload]
        path = process_file(
            dataset_id=DATASET_ID,
            table_id=table_id,
            input_dir="input",
            output_dir="output",
            partition_date=reference_date,
            chunksize=100000,
        )

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=path,
            source_format="parquet",
        )

    return extract_load_data
