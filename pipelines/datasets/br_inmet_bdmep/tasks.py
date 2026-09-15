"""
Tasks for br_inmet_bdmep.
"""

from pipelines.crawler.inmet_bdmep.tasks import (
    extract_last_date_from_source,
    get_base_inmet,
)
from pipelines.utils.metadata.domain import DateFormat, DateOnly, PartBdpro
from pipelines.utils.stage_dispatch import CheckResult, DownloadResult

# ──────────────────────────────────────────────────────────────────────────────
# microdados (issue #1867) — ver constants.py
#
# Particularidade deste dataset: `check_for_update` não é uma checagem
# leve independente — `extract_last_date_from_source` já baixa o ZIP do
# ano corrente (todas as estações do INMET) como efeito colateral, só pra
# inspecionar os nomes dos arquivos e achar a data mais recente. Como
# `check_update` e `download` rodam em pods separados, `download_data`
# baixa o mesmo ZIP de novo (`extract_last_date_from_source`, de novo,
# antes de `get_base_inmet`, que espera os arquivos já no disco local) em
# vez de repassar dado entre pods — mesmo padrão aplicado em br_ibge_ipca
# (ver `levantamento-datasets-por-categoria-de-check.md` no ftwca).
#
# ⚠️ Diferença importante pra br_ibge_ipca: aqui o "período" baixado é o
# ZIP inteiro do ano corrente (todas as ~600 estações), não um recorte
# pequeno de 1 mês — o download é mais pesado. Não foi medido o tamanho
# real nesta migração preliminar (sem teste contra a fonte real); vale
# confirmar que ainda fica confortavelmente abaixo do limiar de 5 GB antes
# de rodar isso de verdade em produção.
# ──────────────────────────────────────────────────────────────────────────────


def microdados_check_for_update() -> CheckResult:
    """Baixa o ZIP do ano corrente (efeito colateral de
    `extract_last_date_from_source`) e devolve a data mais recente entre
    os arquivos baixados."""
    reference_date = extract_last_date_from_source()
    # pyrefly: ignore [bad-argument-type]
    return CheckResult(reference_date=reference_date)


def microdados_download(download_params: dict) -> DownloadResult:
    """Baixa de novo o ZIP do ano corrente (barato o bastante pra repetir,
    ver banner acima) e consolida os CSVs num único arquivo particionado
    por `ano=` (`get_base_inmet`)."""
    reference_date = download_params["reference_date"]
    year = reference_date[:4]

    extract_last_date_from_source()
    filepath = get_base_inmet()

    return DownloadResult(
        coverage=PartBdpro(
            date_column=DateOnly(col="data"),
            date_format=DateFormat.YEAR_MD,
        ).model_dump(),
        data_path=filepath,
        bq_project="basedosdados",
        partition_folders=[f"ano={year}"],
    )
