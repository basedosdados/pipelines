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
# informacao_consolidada (issue #1867) — ver constants.py
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

    # `files` (formato "YYYYMM") pode ter mais de 1 mês quando a ANS
    # atualiza 2 meses no mesmo dia — partition_folders precisa cobrir
    # todos, não só o `reference_date` do check, senão um mês baixado não
    # chega a ser promovido pra prod.
    partition_folders = sorted({f"ano={f[:4]}/mes={f[4:6]}" for f in files})

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=output_filepath,
        # crawler_ans -> parquet_partition grava .parquet, não .csv (default).
        source_format="parquet",
        # to_partitions particiona por ano/mes/sigla_uf/modalidade_operadora,
        # mas só o(s) ano/mes que mudou(aram) por execução — promover o
        # nível ano/mes já carrega todas as sub-partições de uf/modalidade
        # daquele(s) mês(es).
        partition_folders=partition_folders,
    )
