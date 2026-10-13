"""
Tasks de br_inep_enem.

Cada task embrulha uma função de `utils.py`, onde fica a lógica.

Migrado pro pipeline em estágios (staged pipeline): check_update ->
extract_and_load, um par de flows por tabela (`flows.py`). `make_pipeline`
é a fábrica usada lá — fixa `DATASET_ID`, varia só `table_id`.

`get_source_max_date` é o check real: lê na página do INEP até que edição a
fonte publicou, sem baixar nenhum dado — exatamente o que o flow monolítico
antigo fazia antes de decidir se baixava algo.

`backfill_years` não é mais parâmetro do flow: `extract_load_data` lê a
chave opcional `download_params["backfill_years"]`, que só existe quando
alguém dispara o deployment de extract_and_load manualmente com esse valor
— `check_update` nunca a define. Mesmo padrão de `br_ibge_ppm`.
"""

from collections.abc import Callable
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.br_inep_enem import utils
from pipelines.datasets.br_inep_enem.constants import DATASET_ID, constants
from pipelines.utils.metadata.domain import DateFormat
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


@task(retries=3, retry_delay_seconds=30)
def get_source_max_date() -> str:
    """Lê na página do INEP o ano mais recente publicado.

    Returns:
        O ano mais recente, no formato `%Y`.
    """
    return utils.get_source_max_date()


@task(retries=3, retry_delay_seconds=60)
def download_table(table_id: str, ano: str) -> Path:
    """Baixa a edição e extrai o CSV da tabela.

    Args:
        table_id: Slug da tabela.
        ano: Edição a baixar, no formato `%Y`.

    Returns:
        O diretório de entrada com o CSV extraído.
    """
    return utils.download_table(table_id=table_id, ano=ano)


@task
def clean_table(table_id: str, ano: str) -> Path:
    """Limpa e particiona a edição da tabela.

    Args:
        table_id: Slug da tabela.
        ano: Edição a limpar, no formato `%Y`.

    Returns:
        O diretório particionado da edição, no formato esperado por
        `assemble_data_path`.
    """
    return utils.clean_table(table_id=table_id, ano=ano)


@task
def resolve_years(
    backfill_years: list[str] | None, source_max_date: str
) -> list[str]:
    """Decide quais edições a execução vai carregar.

    Args:
        backfill_years: Edições a recarregar, no formato `%Y`, ou None.
        source_max_date: Edição mais recente publicada, no formato `%Y`.

    Returns:
        As edições a carregar, em ordem crescente.
    """
    return utils.resolve_years(
        backfill_years=backfill_years, source_max_date=source_max_date
    )


@task
def assemble_data_path(table_id: str, output_dirs: list[Path]) -> Path:
    """Reúne num só diretório as partições de todas as edições de uma execução.

    Args:
        table_id: Slug da tabela.
        output_dirs: Diretórios devolvidos por `clean_table`, um por edição
            processada nesta execução.

    Returns:
        O diretório compartilhado, no formato esperado por `upload_to_gcs`.
    """
    return utils.assemble_data_path(table_id=table_id, output_dirs=output_dirs)


# ──────────────────────────────────────────────────────────────────────────────
# participantes e resultados — ver constants.py
#
# `get_latest_update` é compartilhado pelas duas tabelas (a mesma página do
# INEP lista a edição mais recente pras duas), daí o `lambda _: get_latest_update`
# em `make_pipeline` em vez de uma fábrica de verdade — ver o docstring de
# `pipeline_factory`. `make_extract_load_data` já varia por `table_id`, porque
# o `file_prefix`/`RENAME`/`COLUMNS` de cada tabela são diferentes.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Lê na página do INEP o ano mais recente publicado.

    Returns:
        `SourceInspection` com a edição mais recente como `reference_date`
        (dia 1 de janeiro do ano, convenção pra cobertura anual). Comparado
        contra `Coverage.DateTimeRange` (`compare_against="coverage"`, o
        default), mesmo alvo de comparação de antes da migração.
    """
    ano = get_source_max_date()
    return SourceInspection(reference_date=date(int(ano), 1, 1))


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        source_max_date = str(
            date.fromisoformat(download_params["reference_date"]).year
        )
        # Só existe num disparo manual direto do deployment de
        # extract_and_load — `check_update` nunca define esta chave.
        backfill_years = download_params.get("backfill_years")

        anos = resolve_years(
            backfill_years=backfill_years, source_max_date=source_max_date
        )

        output_dirs = []
        for ano in anos:
            download_table(table_id=table_id, ano=ano)
            output_dirs.append(clean_table(table_id=table_id, ano=ano))

        data_path = assemble_data_path(
            table_id=table_id, output_dirs=output_dirs
        )

        return ExtractAndLoad(
            coverage=constants.COVERAGE.value[table_id].model_dump(),
            data_path=str(data_path),
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    lambda _: get_latest_update,
    make_extract_load_data,
    # Mesma granularidade do flow monolítico antigo (`run_inep_enem`, que já
    # compara coverage com date_format=DateFormat.YEAR — as duas tabelas são
    # anuais).
    date_format=DateFormat.YEAR,
)
