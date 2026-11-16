"""
Tasks for br_anatel_banda_larga_fixa.
"""

import os
from collections.abc import Callable
from datetime import date

import polars as pl
from prefect import task

from pipelines.datasets.br_anatel_banda_larga_fixa.constants import (
    DATASET_ID,
)
from pipelines.datasets.br_anatel_banda_larga_fixa.constants import (
    constants as anatel_constants,
)
from pipelines.datasets.br_anatel_banda_larga_fixa.utils import (
    download_csv_from_zip,
    treatment,
    treatment_br,
    treatment_municipio,
    treatment_uf,
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
    # pyrefly: ignore [deprecated]
    os.system(
        f"mkdir -p {anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}"
    )
    if table_id == "microdados":
        treatment(csv_path=csv_path, table_id=table_id)

    elif table_id == "densidade_brasil":
        treatment_br(csv_path=csv_path, table_id=table_id)

    elif table_id == "densidade_uf":
        treatment_uf(csv_path=csv_path, table_id=table_id)

    elif table_id == "densidade_municipio":
        treatment_municipio(csv_path=csv_path, table_id=table_id)

    return anatel_constants.TABLES_OUTPUT_PATH.value[table_id]


@task(retries=5, retry_delay_seconds=10)
def get_max_date_in_table_microdados(csv_path: str) -> date:
    df = pl.scan_csv(csv_path, separator=";", infer_schema_length=0)

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
        reference_date = get_max_date_in_table_microdados(csv_path=csv_path)
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
        filepath = join_tables_in_function(
            table_id=table_id, csv_path=csv_path
        )

        return ExtractAndLoad(
            data_path=filepath,
            dump_mode="append",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    # Mesmo formato do flow monolítico antigo (`_run_anatel_banda_larga_fixa`,
    # que já chama `poll_source_for_update_task` com date_format="%Y-%m" — o
    # dado é mensal).
    date_format="%Y-%m",
)
