"""
Tasks for br_ibge_inpc.
"""

from collections.abc import Callable
from datetime import date

from pipelines.crawler.ibge_inflacao.tasks import (
    check_for_updates,
    collect_data_utils,
    json_to_csv,
)
from pipelines.datasets.br_ibge_inpc.constants import COVERAGE, DATASET_ID
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas — ver constants.py
#
# `make_get_latest_update`/`make_extract_load_data` são fábricas parametrizadas
# por `table_id` — a lógica é idêntica pras 4 tabelas (só `geo_level`/
# `classificacao` mudam dentro de `collect_data_utils`/`json_to_csv`). Os
# callables viram atributos de instância de `CheckThenExtractLoadPipeline`,
# nunca são introspectados por `__name__` (diferente dos `@flow` em `flows.py`).
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        collect_data_utils(
            dataset_id=DATASET_ID, table_id=table_id, periodo=None
        )
        reference_date = check_for_updates(
            dataset_id=DATASET_ID, table_id=table_id
        )
        return SourceInspection(reference_date=reference_date)

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        ref = date.fromisoformat(download_params["reference_date"])
        periodo = f"{ref.year}{ref.month:02d}"

        collect_data_utils(
            dataset_id=DATASET_ID, table_id=table_id, periodo=periodo
        )
        filepath = json_to_csv(table_id=table_id, dataset_id=DATASET_ID)

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=filepath,
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    # Mesma granularidade do flow antigo (`_run_ibge_inflacao`, que já
    # compara coverage com date_format="%Y-%m" — o dado é mensal, sem dia).
    date_format="%Y-%m",
)
