"""
Tasks for br_tse_eleicoes — Prefect 3.

Migrado pro pipeline em estágios (staged pipeline): `flows_control`/
`get_data_source_max_date`/`preparing_data` seguem fazendo o trabalho de
sempre sobre `utils.BrTseEleicoes` — só a fiação (check_update/
extract_and_load) muda, movida pra `make_get_latest_update`/
`make_extract_load_data` abaixo.
"""

from collections.abc import Callable
from datetime import datetime
from typing import TypeVar

from prefect import task

from pipelines.datasets.br_tse_eleicoes.constants import (
    DATASET_ID,
    YEAR,
)
from pipelines.datasets.br_tse_eleicoes.utils import flows_catalog
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

T = TypeVar("T")


@task
# Classes de formatação
def flows_control(table_id: str, mode: str, year: str | int) -> type[T]:
    catalog = flows_catalog(year=year)

    # pyrefly: ignore [unsupported-operation]
    select_flow = catalog.get(table_id)["flow"]
    flow = select_flow(
        # pyrefly: ignore [unsupported-operation]
        urls=catalog.get(table_id)["urls"],
        table_id=table_id,
        year=year,
        # pyrefly: ignore [unsupported-operation]
        source=catalog.get(table_id)["source"],
        mode=mode,
    )

    return flow


@task(
    retries=1,
    retry_delay_seconds=60,
)
def download_source_data(flow_class) -> None:
    """Baixa e extrai o(s) ZIP(s) da fonte, populando `df_main`/`df_complement`.

    Chamada de forma independente por `get_latest_update` e por
    `extract_load_data` — cada estágio refaz o download do zero (custo
    duplicado aceito, decisão do pipeline em estágios pra fontes onde o
    check_update já precisa baixar o dado pra descobrir a data).
    """
    flow_class.download_urls()


@task(
    retries=1,
    retry_delay_seconds=60,
)
def get_data_source_max_date(flow_class) -> datetime:
    return flow_class.get_data_source_max_date()


@task
def preparing_data(flow_class) -> str:
    flow_class.formatar()
    return flow_class.path_output


# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas (candidatos, bens_candidato, despesas_candidato,
# receitas_candidato) — ver `utils.flows_catalog`. Sem dependência real entre
# elas (nenhum `ref()` cruzado em models/br_tse_eleicoes/*.sql ou schema.yml,
# só contra diretórios externos como br_bd_diretorios_brasil/data_tempo) — 1
# pipeline (check_update + extract_and_load) por tabela, a regra padrão.
#
# `YEAR` (constants.py) substitui o parâmetro `year` do flow monolítico
# antigo: check_update não recebe parâmetros no pipeline em estágios, então o
# ano do ciclo eleitoral em processamento vira constante, atualizada
# manualmente a cada novo ciclo — mesma manutenção manual de antes, só que
# movida do default do parâmetro pra uma constante.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        flow_class = flows_control(table_id=table_id, mode="prod", year=YEAR)
        download_source_data(flow_class=flow_class)
        reference_date = get_data_source_max_date(flow_class=flow_class)

        return SourceInspection(
            reference_date=reference_date,
            # `Table.Update.latest` (timestamp de execução), não uma
            # competência de `Coverage` — reference_date é `datetime.today()`
            # quando o arquivo baixado tem mais linhas que a tabela em prod
            # pra `YEAR`, ou o `last_modified` da própria tabela em prod
            # quando não há novidade (mesma lógica de
            # `BrTseEleicoes.get_data_source_max_date`).
            compare_against="table_update",
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        flow_class = flows_control(table_id=table_id, mode="prod", year=YEAR)
        download_source_data(flow_class=flow_class)
        data_path = preparing_data(flow_class=flow_class)

        return ExtractAndLoad(
            data_path=data_path,
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)
