"""
Tasks for br_cgu_servidores_executivo_federal.
"""

from collections.abc import Callable

from pipelines.crawler.cgu.tasks import (
    get_current_date_and_download_file,
    partition_data,
    verify_all_url_exists_to_download,
)
from pipelines.crawler.cgu.utils import last_date_in_metadata
from pipelines.datasets.br_cgu_servidores_executivo_federal.constants import (
    COVERAGE,
    DATASET_ID,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

# ──────────────────────────────────────────────────────────────────────────────
# As 7 tabelas — ver constants.py
#
# `make_get_latest_update`/`make_extract_load_data` são fábricas parametrizadas
# por `table_id` — a lógica é idêntica pras 7 tabelas, reaproveitando as
# mesmas tasks compartilhadas de `pipelines.crawler.cgu.tasks`/`.utils` que o
# antigo `_run_cgu_servidores_publicos` já usava.
# ──────────────────────────────────────────────────────────────────────────────

# Mesmo valor fixo que o antigo flow monolítico usava por padrão — nenhum dos
# 7 deployments existentes sobrescrevia `relative_month`.
_RELATIVE_MONTH = 1


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        # Disponibilidade "tudo-ou-nada": o Portal publica os subsistemas de
        # `table_id` em datas escalonadas dentro do mês, então o candidato só
        # vale se TODOS já estiverem publicados — mesmo gate
        # `verify_all_url_exists_to_download` que o antigo
        # `_run_cgu_servidores_publicos` já usava. Quando falta algum
        # subsistema, `reference_date` fica igual à data já conhecida pelo
        # backend: o poll não vê novidade e o ciclo tenta de novo no próximo
        # run, sem baixar nada pela metade.
        last_date, next_date = last_date_in_metadata(
            dataset_id=DATASET_ID,
            table_id=table_id,
            relative_month=_RELATIVE_MONTH,
        )
        url_ok = verify_all_url_exists_to_download(
            dataset_id=DATASET_ID,
            table_id=table_id,
            relative_month=_RELATIVE_MONTH,
        )
        reference_date = next_date if url_ok else last_date
        return SourceInspection(
            reference_date=reference_date, compare_against="coverage"
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Baixa de novo, de forma independente do check_update — mesma
        # fonte, mesmo relative_month, sem reaproveitar o `reference_date` de
        # `download_params` (os dois estágios rodam em execuções separadas,
        # sem compartilhar o disco baixado no check_update). `download_file`
        # (chamado por `get_current_date_and_download_file`) já pula, com
        # warning, qualquer subsistema que não esteja mais disponível nesse
        # intervalo — mesmo comportamento do flow monolítico antigo.
        get_current_date_and_download_file(
            table_id=table_id,
            dataset_id=DATASET_ID,
            relative_month=_RELATIVE_MONTH,
        )
        filepath = partition_data(table_id=table_id, dataset_id=DATASET_ID)

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=filepath,
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    # Mesma granularidade do flow antigo (poll_source_for_update_task já
    # comparava coverage com date_format="%Y-%m" — o dado é mensal).
    date_format="%Y-%m",
)
