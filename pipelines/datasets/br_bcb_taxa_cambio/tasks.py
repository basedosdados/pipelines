"""
Tasks for br_bcb_taxa_cambio.
"""

from datetime import date

from pipelines.crawler.bcb_taxa_cambio.tasks import (
    get_data_taxa_cambio,
    get_source_max_date,
    treat_data_taxa_cambio,
)
from pipelines.datasets.br_bcb_taxa_cambio.constants import (
    COVERAGE,
    TAXA_CAMBIO_TABLE_ID,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# taxa_cambio — ver constants.py
#
# `get_latest_update` é leve de verdade e independente: `get_source_max_date`
# consulta só o dólar numa janela de 15 dias, sem baixar o ano inteiro.
# ──────────────────────────────────────────────────────────────────────────────


def taxa_cambio_get_latest_update() -> SourceInspection:
    reference_date = date.fromisoformat(get_source_max_date())
    return SourceInspection(reference_date=reference_date)


def taxa_cambio_download(download_params: dict) -> ExtractAndLoad:
    ref = date.fromisoformat(download_params["reference_date"])

    get_data_taxa_cambio(table_id=TAXA_CAMBIO_TABLE_ID, ano=ref.year)
    file_info = treat_data_taxa_cambio(table_id=TAXA_CAMBIO_TABLE_ID)

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=file_info["save_output_path"],
    )
