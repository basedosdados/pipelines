"""
Tasks for br_anatel_telefonia_movel.
"""

from collections.abc import Callable

from pipelines.crawler.anatel.telefonia_movel.tasks import (
    get_max_date_in_table_microdados,
    get_semester,
    get_year_full,
    join_tables_in_function,
    unzip,
)
from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    COVERAGE,
    DATASET_ID,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas — ver constants.py
#
# As 4 compartilham o mesmo ZIP de origem (`unzip`, de
# `pipelines/crawler/anatel/telefonia_movel/`) e a mesma resolução de
# ano/semestre (`get_year_full`/`get_semester`, que inspecionam os arquivos
# já descompactados em disco) — só `get_max_date_in_table_microdados`/
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
