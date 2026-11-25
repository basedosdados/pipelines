"""Etapas do pipeline staged de br_bndes_fundos_garantidores."""

from collections.abc import Callable

from pipelines.datasets.br_bndes_fundos_garantidores.constants import (
    constants,
)
from pipelines.datasets.br_bndes_fundos_garantidores.utils import (
    buscar_item_central,
)
from pipelines.utils.stage_dispatch import SourceInspection


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """check_update de `table_id`: mês da posição na central vs cobertura."""
    nome = constants.NOME_CENTRAL.value[table_id]

    def get_latest_update() -> SourceInspection:
        item = buscar_item_central(nome)
        # só o mês conta: a cobertura termina dentro do mês da posição
        return SourceInspection(
            reference_date=item["posicao"].replace(day=1),
            extra_download_params={"posicao": item["posicao"].isoformat()},
        )

    return get_latest_update
