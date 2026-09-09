"""
Tasks for br_ans_beneficiario.
"""

from datetime import date

from pipelines.crawler.ans_beneficiario.tasks import (
    crawler_ans,
    extract_links_and_dates,
    files_to_download,
    get_file_max_date,
)
from pipelines.datasets.br_ans_beneficiario.constants import (
    INFORMACAO_CONSOLIDADA_URL,
)
from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth
from pipelines.utils.stage_dispatch import CheckResult, DownloadResult

# ──────────────────────────────────────────────────────────────────────────────
# informacao_consolidada (issue #1867) — ver constants.py
#
# Diferente de br_ibge_ipca: aqui o check É leve de verdade e independente —
# `extract_links_and_dates` só faz um GET na página de listagem (HTML da
# pasta FTP-like da ANS) e lê datas de "última atualização" já presentes no
# próprio HTML, sem baixar nenhum arquivo de dado. `download_data` refaz essa
# mesma listagem (idem barato) só pra descobrir de novo quais arquivos
# baixar — não dá pra repassar o DataFrame entre pods, mas a chamada em si
# é a mesma leve de sempre, não o download pesado (que só acontece depois,
# em `crawler_ans`).
# ──────────────────────────────────────────────────────────────────────────────


def br_ans_beneficiario_check_for_update() -> CheckResult:
    links_and_dates = extract_links_and_dates(url=INFORMACAO_CONSOLIDADA_URL)
    file_last_date = get_file_max_date(df=links_and_dates)  # "YYYY-MM"
    reference_date = date.fromisoformat(f"{file_last_date}-01")
    return CheckResult(reference_date=reference_date)


def br_ans_beneficiario_download(download_params: dict) -> DownloadResult:
    links_and_dates = extract_links_and_dates(url=INFORMACAO_CONSOLIDADA_URL)
    files = files_to_download(df=links_and_dates, year=None)

    output_filepath = crawler_ans(files=files)

    ref = date.fromisoformat(download_params["reference_date"])
    return DownloadResult(
        coverage=PartBdpro(
            date_column=YearMonth(year="ano", month="mes"),
            date_format=DateFormat.YEAR_MONTH,
        ).model_dump(),
        data_path=output_filepath,
        bq_project="basedosdados",
        # crawler_ans -> parquet_partition grava .parquet, não .csv (default).
        source_format="parquet",
        # to_partitions particiona por ano/mes/sigla_uf/modalidade_operadora,
        # mas só um ano/mes muda por execução — promover o nível ano/mes já
        # carrega todas as sub-partições de uf/modalidade daquele mês.
        partition_folders=[f"ano={ref.year}/mes={ref.month:02d}"],
    )
