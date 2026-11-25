"""
Tasks for br_cgu_beneficios_cidadao.

A fonte (Portal da Transparência) não expõe uma data de última atualização
fora do próprio arquivo: descobrir se há mês novo exige checar a
disponibilidade da URL mês a mês. A variante "check_and_download" que
trataria esse caso à parte foi cancelada — decisão: todo dataset usa
check_update -> extract_and_load.

As 3 tabelas (`novo_bolsa_familia`, `garantia_safra`, `bpc`) têm cada uma
sua própria URL/ZIP — nenhuma depende do download de outra, e não há
`ref()` cross-table entre elas no dbt (`models/br_cgu_beneficios_cidadao/`
só referencia `br_bd_diretorios_*`). Por isso cada tabela vira seu próprio
par check_update/extract_and_load, sem materialização em lote.
"""

from collections.abc import Callable

from pipelines.datasets.br_cgu_beneficios_cidadao.constants import (
    DATASET_ID,
    RELATIVE_MONTH,
)
from pipelines.shared.cgu.tasks import (
    download_file,
    read_and_partition_beneficios_cidadao,
)
from pipelines.shared.cgu.utils import (
    last_date_in_metadata,
    next_available_date,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        # Disponibilidade "tudo-ou-nada": o Portal publica os subsistemas de
        # `table_id` em datas escalonadas dentro do mês, então o candidato só
        # vale se TODOS já estiverem publicados
        last_date_in_api = last_date_in_metadata(
            dataset_id=DATASET_ID,
            table_id=table_id,
        )
        reference_date = next_available_date(
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
        filepath = read_and_partition_beneficios_cidadao(table_id=table_id)

        return ExtractAndLoad(
            data_path=filepath,
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    # Mesma granularidade do flow monolítico antigo (`_run_cgu_beneficios_cidadao`,
    # que já compara coverage com date_format="%Y-%m" — dado mensal, sem dia).
    date_format="%Y-%m",
)
