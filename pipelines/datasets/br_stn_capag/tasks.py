"""
Tasks for br_stn_capag
"""

import shutil
from collections.abc import Callable
from datetime import date
from pathlib import Path

from pipelines.datasets.br_stn_capag.constants import COVERAGE, DATASET_ID
from pipelines.datasets.br_stn_capag.utils import (
    download_table,
    list_resources,
    process_base_calculo,
    process_records,
    select_resources,
    write_partitioned,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        latest = max(
            resource["reference_date"] for resource in list_resources(table_id)
        )
        return SourceInspection(reference_date=latest)

    return get_latest_update


def make_extract_load_data(
    table_id: str,
) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Baixa e trata o(s) recurso(s) da tabela.

        Args:
            download_params: `reference_date` do recurso;
                `historico=True` baixa todos os recursos.
        """
        historico = bool(download_params.get("historico", False))
        reference_date = date.fromisoformat(download_params["reference_date"])

        output_dir = Path("tmp") / "br_stn_capag" / table_id / "output"
        shutil.rmtree(output_dir, ignore_errors=True)
        for resource, path in download_table(
            table_id, None if historico else reference_date
        ):
            df = process_records(path, resource, table_id)
            write_partitioned(df, output_dir, resource)

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=str(output_dir),
            dump_mode="overwrite" if historico else "append",
        )

    return extract_load_data


def make_extract_load_base_calculo(
    table_id: str,
) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Baixa o recurso mais recente e trata a aba Datalake.

        Args:
            download_params: Ignorado: o recurso é sempre o mais recente,
                também no `historico`.
        """
        latest = max(
            resource["reference_date"]
            for resource in select_resources(list_resources(table_id))
        )

        output_dir = Path("tmp") / "br_stn_capag" / table_id / "output"
        shutil.rmtree(output_dir, ignore_errors=True)
        for resource, path in download_table(table_id, latest):
            df = process_base_calculo(path, resource)
            write_partitioned(df, output_dir, resource)

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=str(output_dir),
            dump_mode="overwrite",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)

make_pipeline_base_calculo = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_base_calculo,
)
