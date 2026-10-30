"""
Tasks de br_inep_censo_educacao_superior - Prefect 3.

Liga as funções de `utils.py` ao modelo em estágios: cada tabela fornece um
`get_latest_update` (estágio check_update) e um `extract_load_data` (estágio
extract_and_load). O `CheckThenExtractLoadPipeline` cuida do resto: Poll,
comparação com a BD, disparo dos estágios e upload para o staging.
"""

import datetime
import tempfile
from collections.abc import Callable
from pathlib import Path

from prefect import task

from pipelines.datasets.br_inep_censo_educacao_superior.constants import (
    COVERAGE,
    constants,
)
from pipelines.datasets.br_inep_censo_educacao_superior.utils import (
    anos_alterados,
    anos_publicados,
    download_censo,
    last_modified,
    tratar_tabela,
)
from pipelines.utils.metadata.client import MetadataClient
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

DATASET_ID = constants.DATASET_ID.value


@task(retries=2, retry_delay_seconds=60)
def ler_publicacoes() -> dict[int, datetime.datetime]:
    """Lê o `Last-Modified` do zip de cada edição acompanhada.

    São requisições que leem só o cabeçalho, uma por ano.

    Returns:
        A data de publicação de cada ano.
    """
    return {ano: last_modified(ano) for ano in anos_publicados()}


@task
def ler_ultima_atualizacao(table_id: str) -> datetime.date | None:
    """Lê quando a tabela foi atualizada pela última vez na BD (prod).

    Args:
        table_id: `curso` ou `ies`.

    Returns:
        O `Table.Update.latest`, ou `None` se a tabela não tiver Update.
    """
    return MetadataClient(env="prod").get_table_update_latest(
        DATASET_ID, table_id
    )


@task
def carregar_anos(table_id: str, anos: list[int], work_dir: str) -> str:
    """Baixa e trata cada ano, gravando o CSV de staging de cada um.

    O zip é apagado depois de tratado, para o disco guardar um ano por vez.

    Args:
        table_id: `curso` ou `ies`.
        anos: Edições a carregar.
        work_dir: Diretório de trabalho da execução.

    Returns:
        A raiz da tabela, com uma pasta `ano=<ano>/` por ano carregado.
    """
    input_dir = Path(work_dir) / "input"
    output_dir = Path(work_dir) / "output" / table_id
    input_dir.mkdir(parents=True, exist_ok=True)

    for ano in anos:
        zip_path = download_censo(ano, input_dir)
        csv_path = tratar_tabela(table_id, ano, zip_path, output_dir)
        zip_path.unlink()
        print(f"{table_id} {ano}: {csv_path}")
    return str(output_dir)


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Monta o `get_latest_update` (estágio check_update) de uma tabela.

    A data de referência é o maior `Last-Modified` entre os anos, comparada pelo
    framework com a última atualização da tabela (`compare_against=
    "table_update"`). Os anos a recarregar vão em `extra_download_params`.

    Args:
        table_id: `curso` ou `ies`.

    Returns:
        A função sem argumentos que o `CheckThenExtractLoadPipeline` chama.
    """

    def get_latest_update() -> SourceInspection:
        publicacoes = ler_publicacoes()
        ultima_atualizacao = ler_ultima_atualizacao(table_id)
        anos = anos_alterados(publicacoes, ultima_atualizacao)
        print(
            f"{table_id}: última atualização na BD em {ultima_atualizacao}; "
            f"anos publicados depois disso: {anos or 'nenhum'}"
        )
        return SourceInspection(
            reference_date=max(publicacoes.values()).date(),
            extra_download_params={"anos": anos},
            compare_against="table_update",
        )

    return get_latest_update


def make_extract_load_data(
    table_id: str,
) -> Callable[[dict], ExtractAndLoad]:
    """Monta o `extract_load_data` (estágio extract_and_load) de uma tabela.

    `download_params["anos"]` vem do check_update ou, na primeira execução, é
    passado à mão com todos os anos (ver README).

    Args:
        table_id: `curso` ou `ies`.

    Returns:
        A função que recebe `download_params` e devolve o `ExtractAndLoad`.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        anos = sorted(int(ano) for ano in download_params.get("anos", []))
        if not anos:
            raise ValueError(
                f"{table_id}: download_params sem anos para carregar: "
                f"{download_params}"
            )

        work_dir = tempfile.mkdtemp(prefix=f"{DATASET_ID}_{table_id}_")
        data_path = carregar_anos(table_id, anos, work_dir)
        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=data_path,
            # append substitui só o arquivo de cada ano carregado; overwrite
            # apagaria a tabela de staging inteira.
            dump_mode="append",
            source_format="csv",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID, make_get_latest_update, make_extract_load_data
)
