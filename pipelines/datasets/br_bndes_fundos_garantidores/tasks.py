"""Etapas do pipeline staged de br_bndes_fundos_garantidores."""

import tempfile
from collections.abc import Callable
from pathlib import Path

from pipelines.datasets.br_bndes_fundos_garantidores.constants import (
    constants,
)
from pipelines.datasets.br_bndes_fundos_garantidores.utils import (
    baixar_csv,
    buscar_item_central,
    limpar,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


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


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """extract_and_load de `table_id`: baixa o csv atual e limpa."""
    nome = constants.NOME_CENTRAL.value[table_id]

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        pasta = Path(tempfile.mkdtemp(prefix=f"{table_id}_"))
        item = buscar_item_central(nome)
        csv = baixar_csv(item["url_csv"], pasta / "input" / f"{table_id}.csv")
        saida = limpar(csv, pasta / "output" / table_id)
        # a fonte publica o histórico completo a cada posição
        return ExtractAndLoad(
            data_path=str(saida),
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    constants.DATASET_ID.value,
    make_get_latest_update,
    make_extract_load_data,
    date_format="%Y-%m-%d",
)
