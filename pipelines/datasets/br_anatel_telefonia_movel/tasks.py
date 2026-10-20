"""
Tasks for dataset br_anatel_telefonia_movel
"""

import os
from collections.abc import Callable

import pandas as pd
from prefect import task

from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    COVERAGE,
    DATASET_ID,
)
from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.datasets.br_anatel_telefonia_movel.utils import (
    clean_csv_brasil,
    clean_csv_microdados,
    clean_csv_municipio,
    clean_csv_uf,
    get_year,
    normalize_label,
    unzip_file,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)
from pipelines.utils.utils import log


@task(retries=5, retry_delay_seconds=10)
def join_tables_in_function(table_id, semestre, ano):
    # pyrefly: ignore [deprecated]
    os.system(
        f"mkdir -p {anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}"
    )
    if table_id == "microdados":
        clean_csv_microdados(ano=ano, semestre=semestre, table_id=table_id)

    elif table_id == "densidade_brasil":
        clean_csv_brasil(table_id=table_id)

    elif table_id == "densidade_uf":
        clean_csv_uf(table_id=table_id)

    elif table_id == "densidade_municipio":
        clean_csv_municipio(table_id=table_id)

    return anatel_constants.TABLES_OUTPUT_PATH.value[table_id]


@task(retries=5, retry_delay_seconds=10)
def get_max_date_in_table_microdados(table_id, ano, semestre):
    if table_id == "microdados":
        log(
            f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{ano}_{semestre}S.csv"
        )
        df = pd.read_csv(
            f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{ano}_{semestre}S.csv",
            sep=";",
            encoding="utf-8",
            dtype=str,
        )
        df["data"] = df["Ano"] + "-" + df["Mês"]

        df["data"] = pd.to_datetime(df["data"], format="%Y-%m")

        return df["data"].max()

    else:
        log(
            f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv"
        )

        df = pd.read_csv(
            f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
            sep=";",
            encoding="utf-8",
            dtype=str,
        )

        table_level = {
            "densidade_brasil": "Brasil",
            "densidade_uf": "UF",
            "densidade_municipio": "Municipio",
        }

        level = table_level[table_id]

        geography = df["Nível Geográfico Densidade"]

        df = df[
            geography.map(normalize_label) == normalize_label(level)
        ].copy()

        df["data"] = df["Ano"] + "-" + df["Mês"]

        df["data"] = pd.to_datetime(df["data"], format="%Y-%m")

        return df["data"].max()


@task(retries=5, retry_delay_seconds=10)
def unzip():
    return unzip_file()


@task
def get_year_full(year):
    log("Obtendo o ano...")
    if year is None:
        return get_year()


@task
def get_semester(semester):
    log("Obtendo o semestre...")
    ano = get_year()
    if semester is None:
        if os.path.exists(
            f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{ano}_2S.csv"
        ):
            log("Segundo semestre")
            return 2
        else:
            log("Primeiro semestre")
            return 1


# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas — ver constants.py
#
# As 4 compartilham o mesmo ZIP de origem (`unzip`, definida acima) e a mesma
# resolução de ano/semestre (`get_year_full`/`get_semester`, que inspecionam
# os arquivos já descompactados em disco) — só `get_max_date_in_table_microdados`/
# `join_tables_in_function` variam o que cada chamada lê/extrai do CSV por
# `table_id` (microdados lê `Acessos_Telefonia_Movel_<ano>_<semestre>S.csv`;
# as 3 densidades leem o mesmo `Densidade_Telefonia_Movel.csv`, filtrado por
# geografia). Nenhuma tem teste dbt cross-table (todo `relationships` do
# schema mira os diretórios BD, nunca outra tabela deste dataset — ver
# `models/br_anatel_telefonia_movel/schema.yml`), então a regra padrão desta
# rodada vale sem exceção: 1 `check_update` + 1 `extract_and_load` por
# tabela, cada estágio baixando/descompactando o ZIP de forma independente
# (custo duplicado aceito em troca do padrão único) — mesmo padrão de
# `br_anatel_banda_larga_fixa` (mesma família de fonte).
#
# Nota sobre o poll: `check_update_and_dispatch` (stage_dispatch) comita o
# Update assim que detecta dado novo, ANTES de disparar o extract_and_load —
# diferente do poll "deferido" que o README deste dataset documenta como fix
# aplicado ao flow monolítico antigo (commit só após materialização bem-
# sucedida, pra evitar o bug que travou `densidade_brasil` com o ponteiro
# adiantado). A arquitetura em estágios não tem como adiar o commit — as
# duas etapas são deployments/flow runs separados, disparados via
# `run_deployment(timeout=0)` sem callback de volta — e esse é o mesmo
# trade-off aceito por todo dataset já migrado pra ela (IPCA, au_ato_abr,
# br_anatel_banda_larga_fixa). Se o extract_and_load falhar depois do
# commit, o ponteiro fica adiantado até um `force_run`/backfill manual —
# risco conhecido, não específico deste dataset.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        # Só descobrir a data de referência já exige descompactar o ZIP: a
        # cobertura real (ano/mês máximo publicado) só existe dentro do CSV.
        unzip()
        ano = get_year_full(None)
        semestre = get_semester(None)
        # pyrefly: ignore [no-matching-overload]
        reference_date = get_max_date_in_table_microdados(
            table_id=table_id, ano=ano, semestre=semestre
        )
        # pyrefly: ignore [missing-attribute]
        return SourceInspection(reference_date=reference_date.date())

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Baixa/descompacta o ZIP de novo, independente do check_update —
        # mesmo dado já visto lá, mas os dois estágios rodam em deployments
        # separados, sem estado compartilhado entre eles.
        unzip()
        ano = get_year_full(None)
        semestre = get_semester(None)
        filepath = join_tables_in_function(
            table_id=table_id, ano=ano, semestre=semestre
        )

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
    # Mesmo formato do flow monolítico antigo (`_run_anatel_telefonia_movel`,
    # que já chama `poll_source_for_update_task` com date_format="%Y-%m" — o
    # dado é mensal).
    date_format="%Y-%m",
)
