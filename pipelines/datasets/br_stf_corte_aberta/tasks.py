"""
Tasks for br_stf_corte_aberta.
"""

from pipelines.crawler.stf_corte_aberta.tasks import (
    download_and_transform,
    get_data_source_stf_max_date,
    make_partitions,
)
from pipelines.crawler.stf_corte_aberta.utils import web_scrapping
from pipelines.datasets.br_stf_corte_aberta.constants import COVERAGE
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# decisoes — ver constants.py
#
# A variante "check_and_download" (reaproveitar no extract_and_load o mesmo
# arquivo já baixado pelo check_update) foi cancelada. Decisão: todo dataset
# usa check_update -> extract_and_load, mesmo quando o check precisa baixar o
# dado pra descobrir a data de referência.
#
# `get_data_source_stf_max_date` (crawler/stf_corte_aberta/tasks.py) já
# dispara o `web_scrapping()` (Selenium: abre o painel e clica no botão de
# exportar) por dentro de `check_for_data()` — é o único jeito de descobrir a
# data mais recente nessa fonte, não tem endpoint/listagem leve. Então
# `get_latest_update` já "baixa o que for necessário".
#
# `extract_load_data` chama `web_scrapping()` de novo, de forma independente
# (as duas etapas rodam em flow runs/containers separados, não compartilham o
# arquivo baixado em `/tmp/input/` durante o check) — custo duplicado
# aceito, é a mesma fonte/mesmo botão de exportar.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    reference_date = get_data_source_stf_max_date()
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    web_scrapping()
    df = download_and_transform()
    output_path = make_partitions(df=df)

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=output_path,
    )
