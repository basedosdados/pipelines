"""
Tasks for dataset br_anatel_telefonia_movel
"""

import polars as pl
from prefect import task

from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.datasets.br_anatel_telefonia_movel.utils import (
    clean_csv_densidade,
    clean_csv_microdados,
    download_csv_from_zip,
    normalize_label,
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
def get_max_date_in_table_microdados(table_id, csv_path: str):

    df = pl.scan_csv(csv_path, separator=";", infer_schema_length=0)

    if table_id != "microdados":
        level = anatel_constants.DENSITY_GEOGRAPHIC_LEVEL.value[table_id]

        df = df.filter(
            pl.col("Nível Geográfico Densidade").map_elements(
                normalize_label, return_dtype=pl.Utf8
            )
            == level
        )

    date = (
        df.select(
            pl.date(
                pl.col("Ano").cast(pl.Int32), pl.col("Mês").cast(pl.Int32), 1
            )
        )
        .max()
        .collect()
        .item()
    )

    return date
