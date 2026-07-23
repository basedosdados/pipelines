"""
Tasks Prefect para br_ibge_pnadc — dicionário.

Wrappers finos sobre as funções puras de `utils.py` (que não importam Prefect).
"""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_ibge_pnadc.utils import build_dicionario


@task
def build_dicionario_task(work_dir: str, output_dir: str) -> str:
    """Monta o dicionário e escreve um parquet all-string para upload.

    Parameters
    ----------
    work_dir : str
        Diretório de trabalho para baixar a documentação do IBGE.
    output_dir : str
        Diretório onde escrever o parquet a ser enviado ao staging.

    Returns
    -------
    str
        Caminho do parquet gerado.
    """
    df = build_dicionario(Path(work_dir))

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "dicionario.parquet"

    # build_dicionario já devolve tudo como string; parquet all-string é o que o
    # upload_to_gcs espera (ver "Staging parquet must be all-STRING" nas convenções).
    df.to_parquet(path, index=False)

    return str(path)
