"""Tasks Prefect 3 do br_fnde_fundeb, sobre as funções do `utils.py`.

As tasks de I/O (`check_source_siope`/`download_siope`/`clean_siope`) seguem
compartilhadas pelas duas tabelas de fato (`indicador_estadual`,
`indicador_municipal`) — `make_extract_load_data` as reaproveita por
`table_id`, repetindo download e limpeza do arquivo inteiro pra cada tabela
(1 flow por tabela é o padrão do pipeline em estágios; o `dicionario` não
entra aqui, ver README do conjunto).
"""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.br_fnde_fundeb.constants import constants
from pipelines.datasets.br_fnde_fundeb.utils import (
    clean_all,
    download_product,
    source_update_date,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


@task(retries=2, retry_delay_seconds=60)
def check_source_siope(product_id: int) -> str:
    """Devolve a data de gravação do arquivo na fonte, sem baixá-lo.

    Consulta os metadados do artefato, cuja resposta o poll compara com o
    ``Table.Update.latest``: o arquivo só é baixado quando a plataforma o
    regravou depois da última materialização.

    Args:
        product_id: Id do produto na plataforma — 53 ou 54.

    Returns:
        A data de gravação no formato de ``constants.DATE_FORMAT``.
    """
    return source_update_date(product_id)


@task(retries=2, retry_delay_seconds=60)
def download_siope(work_dir: str, product_id: int) -> str:
    """Baixa o `.txt.gz` de um produto do SIOPE.

    Duas novas tentativas, com intervalo de 60 s: a API não honra `Range`, então
    cada tentativa refaz o download inteiro — 45 MB no produto do histórico.

    Args:
        work_dir: Diretório de trabalho; o arquivo cai em ``<work_dir>/input``.
        product_id: Id do produto na plataforma — 53 ou 54.

    Returns:
        O caminho do arquivo baixado, como string (o Prefect serializa
        resultado de task).
    """
    input_dir = Path(work_dir) / "input"
    return str(download_product(product_id, input_dir))


@task
def clean_siope(work_dir: str, source_path: str) -> dict:
    """Monta as tabelas particionadas a partir do arquivo baixado.

    Args:
        work_dir: Diretório de trabalho; as tabelas caem em
            ``<work_dir>/output``.
        source_path: Caminho do `.txt.gz`, vindo de :func:`download_siope`.

    Returns:
        Um mapa slug da tabela para o diretório particionado, mais
        ``"max_date"`` — o último bimestre presente no arquivo, registrado no
        log da run.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(source_path), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }


# ──────────────────────────────────────────────────────────────────────────────
# Fiação do pipeline em estágios (check_update -> extract_and_load), uma dupla
# de flows por tabela — ver flows.py.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Verifica, sem baixar o arquivo, se a fonte regravou o exercício corrente.

    Compartilhada pelas duas tabelas: a única fonte é o produto 54 inteiro, e
    as duas derivam da mesma gravação. ``compare_against="table_update"`` —
    não ``"coverage"`` — porque a data devolvida é quando o arquivo foi
    gravado na plataforma, não a competência que ele cobre (ver docstring de
    :func:`check_source_siope`).

    Returns:
        A ``SourceInspection`` com a data de gravação da fonte.
    """
    source_date = check_source_siope(
        product_id=constants.PRODUCT_CURRENT.value
    )
    reference_date = datetime.strptime(
        source_date, constants.DATE_FORMAT.value
    ).date()
    return SourceInspection(
        reference_date=reference_date, compare_against="table_update"
    )


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Fábrica do `extract_load_data` de uma tabela de fato.

    Baixa e limpa o arquivo inteiro — ele não é decomponível por tabela — e
    devolve só a partição de `table_id`; chamado uma vez por tabela, o
    download/parse é refeito nas duas (custo duplicado aceito, mesmo padrão
    de `br_ibge_ipca`).

    Args:
        table_id: `constants.TABLE_STATE`/`constants.TABLE_MUNICIPALITY`.

    Returns:
        O callable de `extract_load_data` dessa tabela.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        work_dir = constants.WORK_DIR.value
        source_path = download_siope(
            work_dir=work_dir, product_id=constants.PRODUCT_CURRENT.value
        )
        result = clean_siope(work_dir=work_dir, source_path=source_path)

        return ExtractAndLoad(
            data_path=result[table_id],
            dump_mode="append",
            source_format="parquet",
        )

    return extract_load_data


# `get_latest_update` é compartilhado pelas duas tabelas (`lambda _: ...`, não
# uma fábrica de verdade) — ver docstring de `pipeline_factory`.
make_pipeline = pipeline_factory(
    constants.DATASET_ID.value,
    lambda _: get_latest_update,
    make_extract_load_data,
    date_format=constants.DATE_FORMAT.value,
)
