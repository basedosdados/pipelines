"""
Tasks de br_ibge_ppm.

Cada task embrulha uma função de `utils.py`, onde fica a lógica.

Migrado pro pipeline em estágios (staged pipeline): check_update ->
extract_and_load, um par de flows por tabela (`flows.py`). `make_pipeline`
é a fábrica usada lá — fixa `DATASET_ID`, varia só `table_id`.

`get_source_max_date` é o check real: lê nos metadados do SIDRA até que ano
a fonte publicou, sem baixar nenhum dado — exatamente o que o flow
monolítico antigo fazia antes de decidir se baixava algo.

`backfill_years` não é mais parâmetro do flow: `extract_load_data` lê a
chave opcional `download_params["backfill_years"]`, que só existe quando
alguém dispara o deployment de extract_and_load manualmente com esse valor
— `check_update` nunca a define. Mesmo padrão de `br_bcb_taxa_cambio`
(`anos`) e `br_cgu_despesas_publicas` (`months_back`/`full_refresh`).
"""

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Literal

from prefect import task

from pipelines.datasets.br_ibge_ppm import utils
from pipelines.datasets.br_ibge_ppm.constants import DATASET_ID, constants
from pipelines.utils.metadata.domain import DateFormat
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


@task(retries=3, retry_delay_seconds=30)
def get_source_max_date(table_id: str) -> str:
    """Lê nos metadados do SIDRA até que ano a fonte publicou a tabela.

    Args:
        table_id: Slug da tabela.

    Returns:
        O ano mais recente publicado, no formato `%Y`.
    """
    return utils.get_source_max_date(table_id=table_id)


@task(retries=3, retry_delay_seconds=30)
def get_coverage_max_year(
    dataset_id: str,
    table_id: str,
    env: Literal["dev", "prod", "staging"],
) -> str | None:
    """Lê no backend até que ano a tabela está coberta.

    Args:
        dataset_id: ID do conjunto no BigQuery.
        table_id: Slug da tabela.
        env: Backend a consultar.

    Returns:
        O último ano coberto, no formato `%Y`, ou None se a tabela não tiver
        intervalo de datas registrado.
    """
    return utils.get_coverage_max_year(
        dataset_id=dataset_id, table_id=table_id, env=env
    )


@task
def resolve_years(
    table_id: str,
    backfill_years: list[str] | None,
    source_max_date: str,
    coverage_max_year: str | None,
) -> list[str]:
    """Decide quais anos a execução vai carregar.

    Args:
        table_id: Slug da tabela.
        backfill_years: Anos a recarregar, no formato `%Y`, ou None.
        source_max_date: Ano mais recente publicado, no formato `%Y`.
        coverage_max_year: Último ano coberto em produção, no formato `%Y`, ou
            None se a tabela não tiver cobertura registrada.

    Returns:
        Os anos a carregar, em ordem crescente.
    """
    return utils.resolve_years(
        table_id=table_id,
        backfill_years=backfill_years,
        source_max_date=source_max_date,
        coverage_max_year=coverage_max_year,
    )


@task(retries=3, retry_delay_seconds=60)
def download_table(table_id: str, ano: str) -> Path:
    """Baixa da API do IBGE as séries que compõem a tabela no ano.

    Args:
        table_id: Slug da tabela.
        ano: Ano a baixar, no formato `%Y`.

    Returns:
        O diretório de entrada com os JSONs baixados.
    """
    return utils.download_table(table_id=table_id, ano=ano)


@task
def clean_table(table_id: str, ano: str) -> Path:
    """Junta as séries do ano numa tabela e grava o particionado.

    Args:
        table_id: Slug da tabela.
        ano: Ano a limpar, no formato `%Y`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    return utils.clean_table(table_id=table_id, ano=ano)


@task
def assemble_data_path(table_id: str, output_dirs: list[Path]) -> Path:
    """Reúne num só diretório as partições de todos os anos de uma execução.

    Args:
        table_id: Slug da tabela.
        output_dirs: Diretórios devolvidos por `clean_table`, um por ano
            processado nesta execução.

    Returns:
        O diretório compartilhado, no formato esperado por `upload_to_gcs`.
    """
    return utils.assemble_data_path(table_id=table_id, output_dirs=output_dirs)


# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas — ver constants.py
#
# `make_get_latest_update`/`make_extract_load_data` são fábricas parametrizadas
# por `table_id` — a lógica é idêntica pras 4 tabelas (só a decisão de quais
# anos processar, dentro de `resolve_years`, varia por `table_id`). Os
# callables viram atributos de instância de `CheckThenExtractLoadPipeline`,
# nunca são introspectados por `__name__` (diferente dos `@flow` em `flows.py`).
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        source_max_date = get_source_max_date(table_id=table_id)
        return SourceInspection(
            reference_date=date(int(source_max_date), 1, 1)
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        source_max_date = str(
            date.fromisoformat(download_params["reference_date"]).year
        )
        # Só existe num disparo manual direto do deployment de
        # extract_and_load — `check_update` nunca define esta chave.
        backfill_years = download_params.get("backfill_years")

        coverage_max_year = get_coverage_max_year(
            dataset_id=DATASET_ID, table_id=table_id, env="prod"
        )
        anos = resolve_years(
            table_id=table_id,
            backfill_years=backfill_years,
            source_max_date=source_max_date,
            coverage_max_year=coverage_max_year,
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
    make_get_latest_update,
    make_extract_load_data,
    # Mesma granularidade do flow monolítico antigo (`run_ibge_ppm`, que já
    # compara coverage com date_format=DateFormat.YEAR — a PPM é anual).
    date_format=DateFormat.YEAR,
)
