"""
General purpose functions for the br_anatel_telefonia_movel project of the pipelines
"""

from os import makedirs
from os.path import join

import polars as pl

from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.shared.anatel.utils import normalize_label


def clean_csv_microdados(csv_path: str, table_id: str) -> None:

    df = pl.scan_csv(csv_path, separator=";", infer_schema_length=0)

    renamed_df = df.rename(anatel_constants.RENAME_COLUMNS_MICRODADOS.value)

    selected_cols_df = renamed_df.with_columns(
        pl.col("produto").str.to_lowercase()
    ).select(anatel_constants.ORDER_COLUMNS_MICRODADOS.value)

    one_month_per_row_table = (
        selected_cols_df.select("ano", "mes").unique().collect()
    )

    for year, month in one_month_per_row_table.iter_rows():
        path = join(
            anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
            f"ano={year}",
            f"mes={month}",
        )

        makedirs(path, exist_ok=True)

        df_without_year_and_month = selected_cols_df.filter(
            (pl.col("ano") == year) & (pl.col("mes") == month)
        ).drop("ano", "mes")

        df_without_year_and_month.sink_csv(join(path, "data.csv"))


def clean_csv_densidade(csv_path: str, table_id: str) -> None:

    df = pl.scan_csv(csv_path, separator=";", infer_schema_length=0)

    level = anatel_constants.DENSITY_GEOGRAPHIC_LEVEL.value[table_id]

    filtered_df = (
        df.with_columns(
            normalize_label("Nível Geográfico Densidade").alias("nivel")
        )
        .filter(pl.col("nivel") == level)
        .drop("nivel")
    )

    renamed_df = filtered_df.rename(
        anatel_constants.RENAME_COLUMNS_DENSIDADE.value
    )

    cleaned_df = renamed_df.with_columns(
        pl.col("densidade").str.replace(",", ".")
    ).select(anatel_constants.DENSITY_COLUMNS.value[table_id])

    makedirs(
        anatel_constants.TABLES_OUTPUT_PATH.value[table_id], exist_ok=True
    )

    cleaned_df.sink_csv(
        join(
            anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
            f"{table_id}.csv",
        )
    )
