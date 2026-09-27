"""Tasks Prefect 3 do br_fnde_fundeb, sobre as funções do `utils.py`."""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_fnde_fundeb.utils import (
    clean_all,
    download_product,
    source_update_date,
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
