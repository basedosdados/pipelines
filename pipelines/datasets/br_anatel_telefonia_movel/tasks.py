"""
Tasks for dataset br_anatel_telefonia_movel
"""

from collections.abc import Callable
from datetime import date

import polars as pl
from prefect import task

from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    COVERAGE,
    DATASET_ID,
)
from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.datasets.br_anatel_telefonia_movel.utils import (
    clean_csv_densidade,
    clean_csv_microdados,
    download_csv_from_zip,
    normalize_label,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


@task(retries=5, retry_delay_seconds=10)
def download_csv(table_id: str) -> str:
    """Baixa do ZIP da Anatel o CSV que a tabela lê e devolve o caminho dele."""
    return download_csv_from_zip(table_id)


@task(retries=5, retry_delay_seconds=10)
def join_tables_in_function(table_id: str, csv_path: str) -> str:
    if table_id == "microdados":
        clean_csv_microdados(csv_path=csv_path, table_id=table_id)

    else:
        clean_csv_densidade(csv_path=csv_path, table_id=table_id)

    return anatel_constants.TABLES_OUTPUT_PATH.value[table_id]


@task(retries=5, retry_delay_seconds=10)
def get_max_date_in_table_microdados(table_id: str, csv_path: str) -> date:

    df = pl.scan_csv(csv_path, separator=";", infer_schema_length=0)

    if table_id != "microdados":
        level = anatel_constants.DENSITY_GEOGRAPHIC_LEVEL.value[table_id]

        df = df.filter(
            pl.col("Nível Geográfico Densidade").map_elements(
                normalize_label, return_dtype=pl.Utf8
            )
            == level
        )

    max_date = (
        df.select(
            pl.date(
                pl.col("Ano").cast(pl.Int32), pl.col("Mês").cast(pl.Int32), 1
            )
        )
        .max()
        .collect()
        .item()
    )

    return max_date


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Monta a etapa check_update de uma tabela.

    Args:
        table_id: Tabela que a etapa verifica.

    Returns:
        A função que baixa o CSV da tabela e devolve o mês mais recente dele.
    """

    def get_latest_update() -> SourceInspection:
        csv_path = download_csv(table_id=table_id)
        reference_date = get_max_date_in_table_microdados(
            table_id=table_id, csv_path=csv_path
        )
        return SourceInspection(reference_date=reference_date)

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Monta a etapa extract_and_load de uma tabela.

    Args:
        table_id: Tabela que a etapa baixa e limpa.

    Returns:
        A função que baixa e limpa o CSV da tabela e devolve a pasta de saída.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        csv_path = download_csv(table_id=table_id)
        data_path = join_tables_in_function(
            table_id=table_id, csv_path=csv_path
        )
        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=data_path,
            dump_mode="append",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    date_format="%Y-%m",
)
