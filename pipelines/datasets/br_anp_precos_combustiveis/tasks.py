"""
Tasks for br_anp_precos_combustiveis.

`get_latest_update`/`extract_load_data` reaproveitam as tasks Prefect já
existentes em `pipelines/crawler/anp_precos_combustiveis/tasks.py` — só a
fiação pro pipeline em estágios (staged pipeline) é nova.

Custo duplicado aceito: a fonte só expõe a data de referência baixando o
próprio recorte de 4 semanas (`ultimas-4-semanas-glp.csv`) — não há sinal de
data sem baixar algo primeiro. `get_latest_update` baixa só esse recorte
(`get_data_source_anp_max_date`); `extract_load_data` baixa os 3 CSVs de
novo, de forma independente (`download_and_transform`) — a variante
"check_and_download" que evitaria essa segunda chamada foi cancelada.
"""

from pipelines.crawler.anp_precos_combustiveis.tasks import (
    download_and_transform,
    get_data_source_anp_max_date,
    make_partitions,
)
from pipelines.datasets.br_anp_precos_combustiveis.constants import COVERAGE
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


def get_latest_update() -> SourceInspection:
    reference_date = get_data_source_anp_max_date()
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    df = download_and_transform()
    output_path = make_partitions(df=df)

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=output_path,
    )
