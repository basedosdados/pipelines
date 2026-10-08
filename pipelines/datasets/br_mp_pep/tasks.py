"""
Tasks for br_mp_pep.
"""

from datetime import date

from pipelines.crawler.mp_pep.tasks import (
    clean_data,
    download_xlsx,
    get_page_reference_date,
    make_partitions,
    scraper,
    setup_web_driver,
)
from pipelines.datasets.br_mp_pep.constants import COVERAGE
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# cargos_funcoes — ver constants.py
#
# O check é leve de verdade: `get_page_reference_date` só abre o painel via
# Selenium e lê o mês/ano exibido no título de um elemento da página — não
# baixa nenhum arquivo de dado (levantamento original da issue confirmado
# lendo `pipelines/crawler/mp_pep/tasks.py`, extraído de `is_up_to_date`, que
# fazia essa mesma leitura e já comparava contra o backend; a comparação
# agora é responsabilidade de `poll_source_for_update_task`, na cápsula).
#
# `extract_load_data` baixa só o ano da `reference_date` vinda do check
# (`scraper(year_start=year_end, year_end=year_end)`, um único ano) — mesma
# lógica do flow monolítico antigo, que calculava esse ano a partir do
# relógio (`datetime.now()` menos um dia). Aqui o ano vem da data de
# referência já confirmada pelo check_update, que atravessa as duas etapas
# via `download_params["reference_date"]` — mais preciso que reaproximar
# pelo relógio de parede numa etapa que pode rodar bem depois do check.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    setup_web_driver()
    reference_date = get_page_reference_date()
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    reference_date = date.fromisoformat(download_params["reference_date"])
    year_end = reference_date.year

    setup_web_driver()
    scraper_result = scraper(
        year_start=year_end, year_end=year_end, headless=True
    )
    download_xlsx(scraper_result)
    df = clean_data()
    output_filepath = make_partitions(df)

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=output_filepath,
    )
