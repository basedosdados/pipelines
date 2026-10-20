"""
Tasks de br_ms_sinasc.

Cada task embrulha uma função de `utils.py`, onde fica a lógica.
"""

import datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.br_ms_sinasc import utils
from pipelines.datasets.br_ms_sinasc.constants import (
    COVERAGE,
    MICRODADOS_TABLE_ID,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task(retries=3, retry_delay_seconds=30)
def get_source_max_year() -> str:
    """Lê na fonte até que ano ela publicou.

    Returns:
        O ano mais recente, no formato `%Y`.
    """
    return utils.get_source_max_year()


@task(retries=3, retry_delay_seconds=30)
def resolve_year_source(ano: int) -> str:
    """Diz de qual diretório o ano deve ser baixado.

    Args:
        ano: Ano a resolver.

    Returns:
        A chave da versão em `constants.SOURCES` — `"definitivo"` ou
        `"preliminar"`.
    """
    return utils.resolve_year_source(ano)


@task(retries=3, retry_delay_seconds=60)
def download_table(table_id: str, ano: int, source: str) -> Path:
    """Baixa os arquivos das UFs do ano.

    Args:
        table_id: Slug da tabela.
        ano: Ano a baixar.
        source: Versão do dado, chave de `constants.SOURCES`.

    Returns:
        O diretório de entrada com os arquivos baixados.
    """
    return utils.download_table(table_id=table_id, ano=ano, source=source)


@task
def clean_table(table_id: str, ano: int, source: str) -> Path:
    """Limpa o ano já baixado e grava o particionado.

    Args:
        table_id: Slug da tabela.
        ano: Ano a limpar.
        source: Versão do dado, chave de `constants.SOURCES`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    return utils.clean_table(table_id=table_id, ano=ano, source=source)


# ──────────────────────────────────────────────────────────────────────────────
# microdados — única tabela do dataset hoje.
#
# `get_source_max_year` é a checagem real do flow monolítico antigo: lista o
# diretório do FTP (`list_ftp_years`), sem baixar nenhum `.dbc` — barata o
# bastante pra rodar a cada ciclo de check_update.
#
# `extract_load_data` reaproveita a mesma sequência de antes
# (`resolve_year_source` -> `download_table` -> `clean_table`), só que a
# partir do `reference_date` recebido do check_update em vez de recalcular
# `get_source_max_year()` de novo. O backfill de múltiplos anos do flow
# antigo (parâmetro `anos`) não existe mais — igual `br_ms_sim`, cada
# execução carrega um ano; recarregar outro ano é disparar o deployment de
# extract_and_load manualmente com o `reference_date` daquele ano.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    source_max_year = get_source_max_year()
    reference_date = datetime.date(int(source_max_year), 1, 1)
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    ano = datetime.date.fromisoformat(download_params["reference_date"]).year
    source = resolve_year_source(ano)

    download_table(table_id=MICRODADOS_TABLE_ID, ano=ano, source=source)
    filepath = clean_table(
        table_id=MICRODADOS_TABLE_ID, ano=ano, source=source
    )

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=str(filepath),
        dump_mode="append",
        source_format="csv",
    )
