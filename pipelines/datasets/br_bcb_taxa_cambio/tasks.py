"""
Tasks for br_bcb_taxa_cambio.
"""

from datetime import date

from pipelines.crawler.bcb_taxa_cambio.tasks import (
    get_data_taxa_cambio,
    get_source_max_date,
    treat_data_taxa_cambio,
)
from pipelines.datasets.br_bcb_taxa_cambio.constants import (
    COVERAGE,
    TAXA_CAMBIO_TABLE_ID,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# taxa_cambio — ver constants.py
#
# O check consulta só o dólar numa janela de quinze dias (`get_source_max_date`)
# — as dez moedas saem no mesmo boletim do PTAX, então a data do dólar vale
# pra tabela inteira. Leve: não baixa o ano todo, só os últimos boletins.
#
# `extract_load_data` baixa, pra cada moeda, o ano corrente inteiro (mesmo
# comportamento do flow monolítico antigo) — sem download incremental. Aceita
# opcionalmente `anos` (lista de anos) em `download_params`, pra recarregar
# anos específicos do passado num disparo manual direto do deployment de
# extract_and_load (corrige partição incompleta/duplicada) — nesse modo
# `reference_date` é ignorada; esse caminho nunca é usado pelo check_update,
# só por quem dispara a etapa manualmente.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    reference_date = date.fromisoformat(get_source_max_date())
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    anos = download_params.get("anos")
    anos_alvo: list[int | None] = list(anos) if anos else [None]

    output_paths = []
    for ano in anos_alvo:
        get_data_taxa_cambio(table_id=TAXA_CAMBIO_TABLE_ID, ano=ano)
        file_info = treat_data_taxa_cambio(table_id=TAXA_CAMBIO_TABLE_ID)
        # pyrefly: ignore [bad-index]
        output_paths.append(file_info["save_output_path"])

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=output_paths[-1],
    )
