"""
Tasks for br_cgu_cartao_pagamento.
"""

from collections.abc import Callable

from pipelines.datasets.br_cgu_cartao_pagamento.constants import (
    COVERAGE,
    DATASET_ID,
)
from pipelines.shared.cgu.tasks import (
    get_current_date_and_download_file,
    partition_data,
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
_RELATIVE_MONTH = 1


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        # `get_current_date_and_download_file` já baixa o ZIP do mês
        # candidato pra conseguir confirmar a data de referência (mesma
        # família de portal do br_cgu_beneficios_cidadao: "baixa antes do
        # poll"). `extract_load_data` baixa de novo, de forma independente —
        # custo duplicado aceito, decisão do levantamento original.
        reference_date = get_current_date_and_download_file(
            table_id=table_id,
            dataset_id=DATASET_ID,
            relative_month=_RELATIVE_MONTH,
        )
        return SourceInspection(
            reference_date=reference_date.date(), compare_against="coverage"
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Baixa de novo, de forma independente do check_update — mesma
        # fonte, mesmo relative_month, sem reaproveitar o `reference_date`
        # de `download_params` (os dois estágios rodam em execuções
        # separadas, sem compartilhar o disco baixado no check_update).
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
