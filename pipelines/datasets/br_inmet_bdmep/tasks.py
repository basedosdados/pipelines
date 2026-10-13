"""
Tasks for br_inmet_bdmep.
"""

from pipelines.crawler.inmet_bdmep.tasks import (
    extract_last_date_from_source,
    get_base_inmet,
)
from pipelines.datasets.br_inmet_bdmep.constants import COVERAGE
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# microdados — ver constants.py
#
# `get_latest_update` baixa o ZIP do ano corrente (todas as estações),
# igual `extract_load_data` — repetido de propósito, ver
# `CheckThenExtractLoadPipeline` (`stage_dispatch.py`).
# ──────────────────────────────────────────────────────────────────────────────


def microdados_get_latest_update() -> SourceInspection:
    """Baixa o ZIP do ano corrente (efeito colateral de
    `extract_last_date_from_source`) e devolve a data mais recente entre
    os arquivos baixados."""
    reference_date = extract_last_date_from_source()
    # pyrefly: ignore [bad-argument-type]
    return SourceInspection(reference_date=reference_date)


def microdados_download(download_params: dict) -> ExtractAndLoad:
    """Baixa de novo o ZIP do ano corrente (barato o bastante pra repetir,
    ver banner acima) e consolida os CSVs num único arquivo particionado
    por `ano=` (`get_base_inmet`)."""
    extract_last_date_from_source()
    filepath = get_base_inmet()

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=filepath,
    )
