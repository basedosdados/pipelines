"""
Tasks for br_cgu_cartao_pagamento.
"""

from collections.abc import Callable

from pipelines.datasets.br_cgu_cartao_pagamento.constants import (
    DATASET_ID,
    RELATIVE_MONTH,
)
from pipelines.shared.cgu.tasks import (
    download_file,
    partition_data,
)
from pipelines.shared.cgu.utils import (
    last_date_in_metadata,
    most_recent_available_date,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

# ──────────────────────────────────────────────────────────────────────────────
# As 3 tabelas — ver constants.py
#
# `make_get_latest_update`/`make_extract_load_data` são fábricas parametrizadas
# por `table_id` — a lógica é idêntica pras 3 tabelas, reaproveitando as
# mesmas tasks compartilhadas de `pipelines.shared.cgu.tasks` que o antigo
# `_run_cgu_cartao_pagamento` já usava.
# ──────────────────────────────────────────────────────────────────────────────

# Mesmo valor fixo que o antigo flow monolítico usava por padrão — nenhum dos
# 3 deployments existentes sobrescrevia `relative_month`.


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        # Disponibilidade "tudo-ou-nada": o Portal publica os subsistemas de
        # `table_id` em datas escalonadas dentro do mês, então o candidato só
        # vale se TODOS já estiverem publicados
        last_date_in_api = last_date_in_metadata(
            dataset_id=DATASET_ID,
            table_id=table_id,
        )
        reference_date = most_recent_available_date(
            dataset_id=DATASET_ID,
            table_id=table_id,
            relative_month=RELATIVE_MONTH,
            last_date_in_api=last_date_in_api,
        )
        return SourceInspection(
            reference_date=reference_date, compare_against="coverage"
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        download_file(
            table_id=table_id,
            dataset_id=DATASET_ID,
            reference_date=download_params["reference_date"],
        )
        filepath = partition_data(table_id=table_id, dataset_id=DATASET_ID)

        return ExtractAndLoad(
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
