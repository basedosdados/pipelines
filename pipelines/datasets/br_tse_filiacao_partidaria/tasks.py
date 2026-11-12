"""
Tasks de br_tse_filiacao_partidaria.
"""

from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.br_tse_filiacao_partidaria import utils
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task
def download_table(data_extracao: date) -> Path:
    """Baixa a lista inteira de filiados e grava a staging em CSV.

    Args:
        data_extracao: Data da coleta, que nomeia a partição.

    Returns:
        O diretório raiz das partições.
    """
    return utils.download_table(data_extracao=data_extracao)


def get_latest_update() -> SourceInspection:
    """Etapa check_update da microdados.

    Returns:
        O dia da coleta, comparado com a última atualização da tabela.
    """
    return SourceInspection(
        reference_date=utils.get_source_max_date(),
        compare_against="table_update",
    )


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Etapa extract_and_load da microdados.

    Args:
        download_params: Parâmetros do check_update. O `reference_date` é o
            dia da coleta e vira a `data_extracao`.

    Returns:
        A pasta particionada por `data_extracao` e `sigla_uf`.
    """
    data_extracao = date.fromisoformat(download_params["reference_date"])
    output_dir = download_table(data_extracao=data_extracao)
    return ExtractAndLoad(
        data_path=str(output_dir),
        dump_mode="append",
    )
