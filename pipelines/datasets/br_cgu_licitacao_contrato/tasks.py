"""
Tasks for br_cgu_licitacao_contrato.
"""

from collections.abc import Callable

from pipelines.datasets.br_cgu_licitacao_contrato.constants import (
    DATASET_ID,
    RELATIVE_MONTH,
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
# As 7 tabelas — ver constants.py
#
# `make_get_latest_update`/`make_extract_load_data` são fábricas parametrizadas
# por `table_id` — a lógica é idêntica pras 7 tabelas, compartilhada com
# br_cgu_cartao_pagamento/br_cgu_servidores_publicos via
# `pipelines.shared.cgu.tasks`. `get_current_date_and_download_file` não só
# descobre a data de referência: ela também baixa o ZIP do mês (decisão da
# migração — a variante "check_and_download", que reaproveitaria o download
# do check_update no extract_and_load, foi cancelada). Por isso ela é chamada
# duas vezes, de forma independente, uma em cada estágio — custo duplicado
# aceito. Os callables viram atributos de instância de
# `CheckThenExtractLoadPipeline`, nunca são introspectados por `__name__`
# (diferente dos `@flow` em `flows.py`).
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        reference_date = get_current_date_and_download_file(
            table_id=table_id,
            dataset_id=DATASET_ID,
            relative_month=RELATIVE_MONTH,
        )
        return SourceInspection(reference_date=reference_date)

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Baixa de novo, independente do check_update (ver comentário acima)
        # — `download_params["reference_date"]` não é usado aqui porque
        # `get_current_date_and_download_file` resolve o mesmo mês sozinha,
        # a partir da mesma cobertura registrada no backend (que só muda
        # depois da promoção em build_and_promote).
        get_current_date_and_download_file(
            table_id=table_id,
            dataset_id=DATASET_ID,
            relative_month=RELATIVE_MONTH,
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
    # Mesma granularidade do flow antigo (`_run_cgu_licitacao_contrato`, que
    # já compara/comita com date_format="%Y-%m" — o dado é mensal, sem dia).
    date_format="%Y-%m",
)
