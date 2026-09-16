"""Tasks Prefect 3 do br_sedec_desastres — cascas finas sobre o utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_sedec_desastres.utils import (
    clean_all,
    download_reconhecimentos_vigentes,
)


@task(retries=2, retry_delay_seconds=30)
def download_reconhecimentos(work_dir: str) -> str:
    """Baixa o relatório "Reconhecimentos vigentes" do S2ID.

    Tenta duas vezes de novo: o portal do S2ID derruba requisição sob carga, e o
    export é um postback de formulário que depende de sessão viva.

    Args:
        work_dir: Diretório de trabalho; os arquivos caem em
            ``<work_dir>/input``.

    Returns:
        O caminho do diretório de entrada, como string (o Prefect serializa
        resultado de task).
    """
    input_dir = Path(work_dir) / "input"
    download_reconhecimentos_vigentes(input_dir)
    return str(input_dir)


@task
def clean_reconhecimentos(work_dir: str, input_dir: str) -> dict:
    """Monta a tabela particionada a partir dos exports baixados.

    Args:
        work_dir: Diretório de trabalho; as tabelas caem em
            ``<work_dir>/output``.
        input_dir: Diretório com os exports baixados, vindo de
            :func:`download_reconhecimentos`.

    Returns:
        Um mapa de slug da tabela para o diretório particionado, mais
        ``"max_date"`` — a data do retrato, que alimenta o poll da fonte.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
