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
    COVERAGE,
    INFORMACAO_CONSOLIDADA_URL,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# informacao_consolidada — ver constants.py
#
# Diferente de br_ibge_ipca: aqui o check É leve de verdade e independente —
# `extract_links_and_dates` só faz um GET na página de listagem (HTML da
# pasta FTP-like da ANS) e lê datas de "última atualização" já presentes no
# próprio HTML, sem baixar nenhum arquivo de dado. `extract_load_data` refaz essa
# mesma listagem (idem barato) só pra descobrir de novo quais arquivos
# baixar — não dá pra repassar o DataFrame entre pods, mas a chamada em si
# é a mesma leve de sempre, não o download pesado (que só acontece depois,
# em `crawler_ans`).
# ──────────────────────────────────────────────────────────────────────────────


def br_ans_beneficiario_get_latest_update() -> SourceInspection:
    links_and_dates = extract_links_and_dates(url=INFORMACAO_CONSOLIDADA_URL)
    file_last_date = get_file_max_date(df=links_and_dates)  # "YYYY-MM"
    reference_date = date.fromisoformat(f"{file_last_date}-01")
    return SourceInspection(reference_date=reference_date)


def br_ans_beneficiario_download(download_params: dict) -> ExtractAndLoad:
    links_and_dates = extract_links_and_dates(url=INFORMACAO_CONSOLIDADA_URL)
    files = files_to_download(df=links_and_dates, year=None)

    output_filepath = crawler_ans(files=files)

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=output_filepath,
        # crawler_ans -> parquet_partition grava .parquet, não .csv (default).
        source_format="parquet",
    )
