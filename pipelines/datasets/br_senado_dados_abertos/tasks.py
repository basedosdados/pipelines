"""Tasks de br_senado_dados_abertos.

`make_pipeline` monta o par check_update/extract_and_load de cada tabela.
"""

import os
import tempfile
from collections.abc import Callable
from datetime import date

import pandas as pd
from prefect import task

from pipelines.datasets.br_senado_dados_abertos.constants import (
    PARTITIONED_CONSTANTS,
    TABLES_CONSTANTS,
    constants,
)
from pipelines.datasets.br_senado_dados_abertos.utils import (
    clean_bloco,
    clean_comissao,
    clean_discurso,
    clean_lideranca,
    clean_mesa,
    clean_orientacao,
    clean_partido,
    clean_processo,
    clean_relatoria,
    clean_senador,
    clean_senador_cargo,
    clean_senador_comissao,
    clean_senador_filiacao,
    clean_senador_mandato,
    clean_votacao,
    clean_votacao_comissao,
    get_comissao_siglas,
    get_senador_codes,
    recent_window,
    write_table,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


@task
def extract_table(table_id: str) -> pd.DataFrame:
    """Extrai e limpa uma tabela na janela de rotina.

    Args:
        table_id: nome da tabela.

    Returns:
        A tabela, com as colunas da arquitetura. Os cadastros e as votações
        em comissão vêm inteiros; as outras séries, só nos anos da janela.

    Raises:
        ValueError: se a tabela não for do conjunto.
    """
    years = recent_window(constants.REFRESH_PRIOR_YEARS.value)
    match table_id:
        case "partido":
            return clean_partido()
        case "bloco":
            return clean_bloco()
        case "lideranca":
            return clean_lideranca()
        case "comissao":
            return clean_comissao()
        case "mesa":
            return clean_mesa()
        case "senador":
            return clean_senador()
        case "senador_mandato":
            return clean_senador_mandato(get_senador_codes(clean_senador()))
        case "senador_filiacao":
            return clean_senador_filiacao(get_senador_codes(clean_senador()))
        case "senador_comissao":
            return clean_senador_comissao(get_senador_codes(clean_senador()))
        case "senador_cargo":
            return clean_senador_cargo(get_senador_codes(clean_senador()))
        case "votacao":
            return clean_votacao(years)[0]
        case "votacao_parlamentar":
            return clean_votacao(years)[1]
        case "votacao_orientacao_bancada":
            return clean_orientacao(years)
        case "processo":
            return clean_processo(years)
        case "relatoria":
            return clean_relatoria(years)
        case "votacao_comissao":
            siglas = get_comissao_siglas(clean_comissao())
            return clean_votacao_comissao(siglas)[0]
        case "votacao_comissao_parlamentar":
            siglas = get_comissao_siglas(clean_comissao())
            return clean_votacao_comissao(siglas)[1]
        case "discurso":
            return clean_discurso(years)
    raise ValueError(f"tabela fora do conjunto: {table_id}")


@task
def get_max_date(table_id: str, df: pd.DataFrame) -> date:
    """Lê a maior data da coluna de data de uma série.

    Args:
        table_id: nome da série.
        df: a série, extraída por `extract_table`.

    Returns:
        A maior data, ou `constants.NO_NEW_DATA_DATE` se a série vier sem
        data.
    """
    dates = df[PARTITIONED_CONSTANTS[table_id].DATE_COLUMN.value].dropna()
    if dates.empty:
        return constants.NO_NEW_DATA_DATE.value
    return date.fromisoformat(dates.max())


@task
def save_table(table_id: str, df: pd.DataFrame) -> str:
    """Grava uma tabela em parquet num diretório temporário.

    Args:
        table_id: nome da tabela.
        df: a tabela, extraída por `extract_table`.

    Returns:
        O diretório da tabela, no formato esperado por `upload_to_gcs`.
    """
    work_dir = tempfile.mkdtemp(prefix=f"br_senado_dados_abertos_{table_id}_")
    return write_table(os.path.join(work_dir, "output"), table_id, df)


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Monta o check_update de uma tabela.

    Args:
        table_id: nome da tabela.

    Returns:
        Função que lê a data de referência da fonte. Um cadastro não tem
        data de publicação: usa a data de hoje, comparada com o
        `Table.Update`. Uma série usa a maior data da janela, comparada com
        a cobertura.
    """

    def get_latest_update() -> SourceInspection:
        """Lê a data de referência da fonte."""
        if table_id in constants.DIMS.value:
            return SourceInspection(
                reference_date=date.today(), compare_against="table_update"
            )
        df = extract_table(table_id=table_id)
        return SourceInspection(
            reference_date=get_max_date(table_id=table_id, df=df)
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Monta o extract_and_load de uma tabela.

    Args:
        table_id: nome da tabela.

    Returns:
        Função que extrai a tabela e devolve o parquet e a cobertura.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Extrai a tabela e devolve o parquet e a cobertura."""
        df = extract_table(table_id=table_id)
        return ExtractAndLoad(
            coverage=TABLES_CONSTANTS[table_id].COVERAGE.value.model_dump(),
            data_path=save_table(table_id=table_id, df=df),
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    constants.DATASET_ID.value,
    make_get_latest_update,
    make_extract_load_data,
)
