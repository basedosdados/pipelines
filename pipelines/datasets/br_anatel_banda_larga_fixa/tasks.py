"""
Tasks for br_anatel_banda_larga_fixa.
"""

import os
from collections.abc import Callable

import pandas as pd
from prefect import task

from pipelines.datasets.br_anatel_banda_larga_fixa.constants import (
    COVERAGE,
    DATASET_ID,
)
from pipelines.datasets.br_anatel_banda_larga_fixa.constants import (
    constants as anatel_constants,
)
from pipelines.datasets.br_anatel_banda_larga_fixa.utils import (
    get_year,
    treatment,
    treatment_br,
    treatment_municipio,
    treatment_uf,
    unzip_file,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)
from pipelines.utils.utils import log


@task(retries=5, retry_delay_seconds=10)
def join_tables_in_function(table_id: str, ano):
    # pyrefly: ignore [deprecated]
    os.system(
        f"mkdir -p {anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}"
    )
    if table_id == "microdados":
        treatment(ano=ano, table_id=table_id)

    elif table_id == "densidade_brasil":
        treatment_br(table_id=table_id)

    elif table_id == "densidade_uf":
        treatment_uf(table_id=table_id)

    elif table_id == "densidade_municipio":
        treatment_municipio(table_id=table_id)

    return anatel_constants.TABLES_OUTPUT_PATH.value[table_id]


@task(retries=5, retry_delay_seconds=10)
def get_max_date_in_table_microdados(table_id: str, ano: int):
    if table_id == "microdados":
        log(
            f"{anatel_constants.INPUT_PATH.value}Acessos_Banda_Larga_Fixa_{ano}.csv"
        )
        df = pd.read_csv(
            f"{anatel_constants.INPUT_PATH.value}Acessos_Banda_Larga_Fixa_{ano}.csv",
            sep=";",
            encoding="utf-8",
            dtype=str,
        )
        df["data"] = df["Ano"] + "-" + df["Mês"]

        df["data"] = pd.to_datetime(df["data"], format="%Y-%m")

        return df["data"].max()

    else:
        log(
            f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv"
        )

        df = pd.read_csv(
            f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
            sep=";",
            encoding="utf-8",
            dtype=str,
        )
        df["data"] = df["Ano"] + "-" + df["Mês"]

        df["data"] = pd.to_datetime(df["data"], format="%Y-%m")

        return df["data"].max()


@task
def get_year_and_unzip(day):
    if day is None:
        unzip_file()

    return get_year()


# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas — ver constants.py
#
# As 4 compartilham o mesmo ZIP de origem (~1 GB) e a mesma lógica de
# check/download/unzip/clean (`get_year_and_unzip`/
# `get_max_date_in_table_microdados`/`join_tables_in_function`, definidas
# acima) — só o `table_id` muda o que cada chamada lê/extrai do ZIP. Nenhuma
# delas tem teste dbt cross-table (todo `relationships` do schema mira os
# diretórios BD, nunca outra tabela deste dataset — ver
# `models/br_anatel_banda_larga_fixa/schema.yml`), então a regra padrão desta
# rodada vale sem exceção: 1 `check_update` + 1 `extract_and_load` por
# tabela, cada estágio baixando/descompactando o ZIP de forma independente
# (custo duplicado aceito em troca do padrão único).
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        # Só descobrir a data de referência já exige descompactar o ZIP: a
        # cobertura real (ano/mês máximo publicado) só existe dentro do CSV.
        ano = get_year_and_unzip(day=None)
        # pyrefly: ignore [no-matching-overload]
        reference_date = get_max_date_in_table_microdados(
            table_id=table_id, ano=ano
        )
        return SourceInspection(reference_date=reference_date.date())

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Baixa/descompacta o ZIP de novo, independente do check_update —
        # mesmo dado já visto lá, mas os dois estágios rodam em deployments
        # separados, sem estado compartilhado entre eles.
        ano = get_year_and_unzip(day=None)
        filepath = join_tables_in_function(table_id=table_id, ano=ano)

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=filepath,
            dump_mode="append",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    # Mesmo formato do flow monolítico antigo (`_run_anatel_banda_larga_fixa`,
    # que já chama `poll_source_for_update_task` com date_format="%Y-%m" — o
    # dado é mensal).
    date_format="%Y-%m",
)
