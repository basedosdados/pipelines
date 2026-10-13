"""
Tasks for br_cvm_fi.
"""

from collections.abc import Callable
from datetime import date

from pipelines.crawler.cvm.tasks import (
    clean_cvm_data,
    download_unzip,
    extract_links_and_dates,
    generate_links_to_download,
)
from pipelines.datasets.br_cvm_fi.constants import (
    DATASET_ID,
    DATE_COLUMN_BY_TABLE,
)
from pipelines.utils.metadata.domain import AllBdpro, DateFormat, DateOnly
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

# ──────────────────────────────────────────────────────────────────────────────
# As 6 tabelas — ver constants.py
#
# `make_get_latest_update`/`make_extract_load_data` reaproveitam por completo
# a lógica de scraping/parsing de `pipelines/crawler/cvm/tasks.py` (mesma do
# antigo `_run_cvm_fi`, monolítico): a fonte publica uma listagem HTML com
# nome de arquivo + mtime de última atualização.
#
# `get_latest_update` só espia essa listagem (`extract_links_and_dates`) pra
# achar o mtime máximo — não baixa nada. `extract_load_data` refaz a mesma
# listagem (chamada HTTP barata, sem download) e baixa só os arquivos cujo
# mtime bate com `reference_date` (`generate_links_to_download` +
# `download_unzip`), exatamente como `_run_cvm_fi` fazia depois do poll.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        _, max_date = extract_links_and_dates(table_id=table_id)
        reference_date = date.fromisoformat(max_date)

        return SourceInspection(
            reference_date=reference_date,
            # O `max_date` é o mtime máximo da listagem, não uma competência
            # com baseline de coverage confiável — mesmo
            # compare_against="table_update" que `_run_cvm_fi` já usava.
            compare_against="table_update",
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        reference_date = download_params["reference_date"]

        df, _ = extract_links_and_dates(table_id=table_id)
        # pyrefly: ignore [no-matching-overload]
        arquivos = generate_links_to_download(df=df, max_date=reference_date)

        input_filepath = download_unzip(table_id=table_id, files=arquivos)
        output_filepath = clean_cvm_data(
            input_dir=input_filepath, table_id=table_id, config_key=table_id
        )

        coverage = AllBdpro(
            date_column=DateOnly(col=DATE_COLUMN_BY_TABLE[table_id]),
            date_format=DateFormat.YEAR_MD,
        )

        return ExtractAndLoad(
            coverage=coverage.model_dump(),
            data_path=output_filepath,
            dump_mode="append",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)
