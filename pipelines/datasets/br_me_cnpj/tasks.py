"""
Tasks for br_me_cnpj.
"""

from collections.abc import Callable
from datetime import datetime

from pipelines.crawler.me_cnpj.constants import constants as constants_cnpj
from pipelines.crawler.me_cnpj.tasks import get_data_source_max_date, main
from pipelines.utils.metadata.domain import (
    DateFormat,
    NonHistorical,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.stage_dispatch import CheckResult, DownloadResult

# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas (issue #1867) — ver constants.py
#
# `get_data_source_max_date` é uma checagem leve de verdade — faz um
# PROPFIND (WebDAV) na listagem de diretórios da Receita Federal
# (`crawler/me_cnpj/utils.py::data_url`), sem baixar nenhum arquivo de
# dado. É a MESMA chamada pras 4 tabelas (o índice de pastas é único pro
# dump inteiro de CNPJ) — chamada de novo em `download_data` (barata, 1
# request HTTP) em vez de repassar entre pods, mesmo padrão do
# `br_ibge_ipca`.
#
# `_TABELAS_IDX` mapeia `table_id` (usado no backend/BigQuery, minúsculo)
# pro índice da lista `constants_cnpj.TABELAS` (capitalizada, usada pelas
# funções antigas de download/parsing) — mesmo mapeamento que
# `crawler/me_cnpj/flows.py::_run_me_cnpj` já usava.
# ──────────────────────────────────────────────────────────────────────────────

_TABELAS_IDX = {
    "empresas": 0,
    "socios": 1,
    "estabelecimentos": 2,
    "simples": 3,
}


def make_check_for_update(table_id: str) -> Callable[[], CheckResult]:
    def check_for_update() -> CheckResult:
        max_folder_date, _max_last_modified_date = get_data_source_max_date()
        # pyrefly: ignore [bad-argument-type]
        reference_date = datetime.strptime(max_folder_date, "%Y-%m").date()
        return CheckResult(reference_date=reference_date)

    return check_for_update


def make_download_data(table_id: str) -> Callable[[dict], DownloadResult]:
    def download_data(download_params: dict) -> DownloadResult:
        max_folder_date, max_last_modified_date = get_data_source_max_date()
        idx = _TABELAS_IDX[table_id]
        tabelas = constants_cnpj.TABELAS.value[idx : idx + 1]

        output_filepath = main(
            tabelas=tabelas,
            max_folder_date=max_folder_date,
            max_last_modified_date=max_last_modified_date,
        )

        if table_id == "simples":
            # historical_database=False (sem coluna de data confiável) →
            # NonHistorical, mesma exceção do flow antigo.
            coverage = NonHistorical().model_dump()
        else:
            coverage = PartBdpro(
                date_column=YearMonth(year="ano", month="mes"),
                date_format=DateFormat.YEAR_MONTH,
            ).model_dump()

        return DownloadResult(
            coverage=coverage,
            data_path=output_filepath,
            bq_project="basedosdados",
        )

    return download_data
