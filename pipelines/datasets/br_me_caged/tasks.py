"""
Tasks for br_me_caged.
"""

from collections.abc import Callable
from datetime import date

from pipelines.crawler.me_caged.constants import constants as caged_constants
from pipelines.crawler.me_caged.tasks import (
    build_partitions,
    build_table_paths,
    crawl_novo_caged_ftp,
    generate_yearmonth_range,
    get_source_last_date,
    get_table_last_date,
)
from pipelines.datasets.br_me_caged.constants import DATASET_ID
from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth
from pipelines.utils.stage_dispatch import CheckResult, DownloadResult

# ──────────────────────────────────────────────────────────────────────────────
# As 3 tabelas (issue #1867) — ver constants.py
#
# Diferente de br_ibge_ipca, o check aqui é de verdade leve e independente:
# `get_source_last_date` só faz uma listagem de diretório FTP (`ftp.nlst()`),
# sem baixar nenhum arquivo — e é a mesma checagem pras 3 tabelas (a fonte
# publica um release mensal só, com os 3 tipos de arquivo juntos no mesmo
# diretório ano/mês). Por isso `check_for_update` não precisa de fábrica
# por `table_id` — é uma função só, compartilhada pelas 3 pipelines.
#
# `download_data` (fábrica por `table_id`, já que cada tabela filtra um
# tipo de arquivo diferente — CAGEDMOV/CAGEDFOR/CAGEDEXC, ver
# `crawl_novo_caged_ftp`) reproduz a lógica original de catch-up: busca de
# novo a coverage atual da tabela (`get_table_last_date`) e baixa **todos**
# os meses entre ela e o período achado pelo check (`generate_yearmonth_range`)
# — pode ser mais de 1 mês se o pipeline ficou atrasado, igual o flow antigo.
#
# `partition_folders`: construído como o produto cartesiano
# ano=/mes=/sigla_uf= pra cada (yearmonth, UF) — `build_partitions` escreve
# um arquivo por UF que tiver dado naquele mês, então essa lista é um teto
# (algumas combinações podem não existir de fato); não verificado contra
# execução real ainda.
# ──────────────────────────────────────────────────────────────────────────────


def br_me_caged_check_for_update() -> CheckResult:
    reference_date = get_source_last_date()
    # pyrefly: ignore [bad-argument-type]
    return CheckResult(reference_date=reference_date)


def make_download_data(table_id: str) -> Callable[[dict], DownloadResult]:
    def download_data(download_params: dict) -> DownloadResult:
        source_last_date = date.fromisoformat(
            download_params["reference_date"]
        )
        table_last_date = get_table_last_date(DATASET_ID, table_id)
        _input_dir, output_dir = build_table_paths(table_id)
        yearmonths = generate_yearmonth_range(
            table_last_date, source_last_date
        )

        for yearmonth in yearmonths:
            crawl_novo_caged_ftp(yearmonth, table_id)

        filepath = build_partitions(
            table_id=table_id, table_output_dir=output_dir
        )

        partition_folders = [
            f"ano={yearmonth[:4]}/mes={int(yearmonth[4:6])}/sigla_uf={uf}"
            for yearmonth in yearmonths
            for uf in caged_constants.UF_DICT.value.values()
        ]

        return DownloadResult(
            coverage=PartBdpro(
                date_column=YearMonth(year="ano", month="mes"),
                date_format=DateFormat.YEAR_MONTH,
            ).model_dump(),
            data_path=filepath,
            bq_project="basedosdados",
            partition_folders=partition_folders,
        )

    return download_data
