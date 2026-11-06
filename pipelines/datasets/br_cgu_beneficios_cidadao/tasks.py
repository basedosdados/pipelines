"""
Tasks for br_cgu_beneficios_cidadao.

A fonte (Portal da Transparência) não expõe uma data de última atualização
fora do próprio arquivo: descobrir se há mês novo exige checar a
disponibilidade do ZIP mês a mês e baixá-lo quando disponível
(`get_current_date_and_download_file`, em `pipelines/shared/cgu/tasks.py`
— o ZIP é assíncrono, `source_url_is_available` faz polling até ficar
pronto). A variante "check_and_download" que trataria esse caso à parte foi
cancelada — decisão: todo dataset usa check_update -> extract_and_load,
mesmo quando o check precisa baixar dado pra descobrir a data.
`extract_load_data` baixa a fonte de novo, de forma independente — custo
duplicado aceito.

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
    SOURCE_FORMAT,
)
from pipelines.shared.cgu.tasks import (
    get_current_date_and_download_file,
    read_and_partition_beneficios_cidadao,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        reference_datetime = get_current_date_and_download_file(
            table_id=table_id,
            dataset_id=DATASET_ID,
            relative_month=RELATIVE_MONTH,
        )
        return SourceInspection(reference_date=reference_datetime.date())

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # `reference_date` em `download_params` não é usado pra parametrizar
        # o download: `get_current_date_and_download_file` recalcula o mês
        # a baixar sozinho, a partir da última data em metadados + relative_month
        # — mesmo cálculo que o check_update acabou de fazer.
        get_current_date_and_download_file(
            table_id=table_id,
            dataset_id=DATASET_ID,
            relative_month=RELATIVE_MONTH,
        )
        filepath = read_and_partition_beneficios_cidadao(table_id=table_id)

        return ExtractAndLoad(
            data_path=filepath,
            source_format=SOURCE_FORMAT[table_id],
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
