"""
Tasks for br_bcb_agencia.
"""

from datetime import date, datetime

from pipelines.crawler.bcb_agencia.tasks import (
    clean_data,
    download_table,
    extract_urls_list,
    get_documents_metadata,
    get_latest_file,
)
from pipelines.datasets.br_bcb_agencia.constants import (
    AGENCIA_TABLE_ID,
    COVERAGE,
    DATASET_ID,
)
from pipelines.utils.metadata.tasks import task_get_api_most_recent_date
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# agencia — ver constants.py
#
# O check busca o metadado do BCB (`get_documents_metadata`) e lê a data do
# documento mais recente (`get_latest_file`) — leve, sem baixar nenhum arquivo
# de dado. `extract_load_data` refaz essa mesma busca de metadado (idem
# barata, com retry) só pra reconstruir a lista de URLs a baixar: diferente
# de br_ibge_ipca/br_ans_beneficiario, aqui pode haver mais de um mês de
# atraso acumulado, então o download real usa `task_get_api_most_recent_date`
# (o que já está publicado, mesma leitura que o flow monolítico antigo fazia
# logo após confirmar novidade) como baseline e `extract_urls_list` pra achar
# todos os meses faltantes entre essa baseline e a `reference_date` do check
# — mesma lógica do flow monolítico antigo, só reparticionada entre as duas
# etapas.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    documents_metadata = get_documents_metadata()
    if documents_metadata is None:
        raise RuntimeError(
            "BCB metadata was not loaded! It was not possible to determine "
            "if the dataset is up to date."
        )

    _, data_source_max_date = get_latest_file(documents_metadata)
    reference_date = datetime.strptime(data_source_max_date, "%Y-%m").date()
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    reference_date = date.fromisoformat(download_params["reference_date"])

    documents_metadata = get_documents_metadata()
    if documents_metadata is None:
        raise RuntimeError(
            "BCB metadata was not loaded! It was not possible to determine "
            "if the dataset is up to date."
        )

    api_max_date = task_get_api_most_recent_date(
        dataset_id=DATASET_ID,
        table_id=AGENCIA_TABLE_ID,
        date_format="%Y-%m",
        api_mode="prod",
    )

    # pyrefly: ignore [no-matching-overload]
    urls_list = extract_urls_list(
        documents_metadata,
        reference_date,
        api_max_date,
        date_format="%Y-%m",
    )

    for url in urls_list:
        download_table(url=url)

    filepath = clean_data()

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=filepath,
    )
