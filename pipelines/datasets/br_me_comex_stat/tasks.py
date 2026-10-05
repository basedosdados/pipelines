"""
Tasks for br_me_comex_stat.
"""

from collections.abc import Callable
from datetime import date

from pipelines.crawler.me_comex_stat.constants import (
    constants as comex_constants,
)
from pipelines.crawler.me_comex_stat.tasks import (
    clean_br_me_comex_stat,
    download_br_me_comex_stat,
    parse_last_date,
)
from pipelines.datasets.br_me_comex_stat.constants import (
    COVERAGE,
    DATASET_ID,
    TABLE_SPECS,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas (município/NCM x exportação/importação) — ver constants.py
#
# `get_latest_update` é a MESMA função pras 4 tabelas — a fonte de check é
# uma página de metadados única (`DOWNLOAD_LINK`), compartilhada, não por
# tabela (o flow antigo já comentava isso: "A fonte é uma só para as
# quatro tabelas"). Diferente de `br_ibge_ipca`, aqui o check é leve de
# verdade: `parse_last_date` só faz scrape de HTML de uma página de
# metadados, não baixa o dado bruto (~poucos KB de HTML) — não precisa da
# lógica de "baixar duas vezes" usada lá.
#
# `extract_load_data` precisa de fábrica por tabela (`table_name`/`table_type`
# variam). `clean_br_me_comex_stat` particiona por `ano/mes` (tabelas NCM)
# ou `ano/mes/sigla_uf` (tabelas de município, várias UFs por arquivo
# baixado) — `discover_partition_folders` (stage_dispatch.py) descobre as
# pastas-folha realmente escritas em disco, sem hardcoded as UFs
# presentes no arquivo.
# ──────────────────────────────────────────────────────────────────────────────


def br_me_comex_stat_get_latest_update() -> SourceInspection:
    last_date = parse_last_date(link=comex_constants.DOWNLOAD_LINK.value)
    # pyrefly: ignore [missing-attribute]
    year, month = last_date.split("-")
    return SourceInspection(reference_date=date(int(year), int(month), 1))


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    spec = TABLE_SPECS[table_id]
    table_name = spec["table_name"]
    table_type = spec["table_type"]

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        ref = date.fromisoformat(download_params["reference_date"])
        year_download = f"{ref.year}-{ref.month:02d}"

        download_br_me_comex_stat(
            table_name=table_name, year_download=year_download
        )
        filepath = clean_br_me_comex_stat(
            path=comex_constants.PATH.value,
            table_type=table_type,
            table_name=table_name,
        )

        # `clean_br_me_comex_stat` está tipado (errado) como -> pd.DataFrame,
        # mas devolve `str` de verdade (mesma pendência do código original,
        # já marcada lá com `# pyrefly: ignore [bad-return]`).
        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            # pyrefly: ignore [bad-argument-type]
            data_path=filepath,
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    # Mesma função pras 4 tabelas -- a fonte de check é única,
    # compartilhada (ver banner acima).
    lambda _table_id: br_me_comex_stat_get_latest_update,
    make_extract_load_data,
    date_format="%Y-%m",
)
