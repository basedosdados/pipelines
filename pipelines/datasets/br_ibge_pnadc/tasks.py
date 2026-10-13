"""
Tasks para br_ibge_pnadc.

`get_latest_update`/`extract_load_data`: lógica da tabela `microdados`,
migrada pro pipeline em estágios (staged pipeline) — ver `flows.py`.
`build_dicionario_task`: wrapper fino sobre as funções puras de `utils.py`
(que não importam Prefect), pra tabela `dicionario` (ainda não migrada).
"""

from pathlib import Path

from prefect import task

from pipelines.crawler.ibge_pnadc.tasks import (
    build_partitions,
    build_table_paths,
    get_data_source_date_and_url,
)
from pipelines.datasets.br_ibge_pnadc.constants import (
    MICRODADOS_COVERAGE,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_ibge_pnadc.utils import build_dicionario
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection
from pipelines.utils.to_download.tasks import download_async

# ──────────────────────────────────────────────────────────────────────────────
# microdados
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Acha o arquivo de microdados mais recente publicado no FTP do IBGE.

    `get_data_source_date_and_url` já faz a checagem de frescor em si (lista
    a pasta do ano corrente e acha o arquivo mais recentemente modificado) —
    aqui só se empacota o resultado pro `check_update`.

    Returns:
        `SourceInspection` com a data de modificação do arquivo encontrado
        (comparada contra `Table.Update.latest`, não coverage — ver
        `compare_against`) e a URL do arquivo em `extra_download_params`,
        pro `extract_and_load` baixar sem precisar relistar o FTP.
    """
    last_modified, url = get_data_source_date_and_url(year=None, quarter=None)
    return SourceInspection(
        reference_date=last_modified.date(),
        extra_download_params={"url": url},
        compare_against="table_update",
    )


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Baixa o zip de microdados encontrado pelo check_update e particiona.

    Args:
        download_params: recebido do check_update — usa `url`, a URL do
            arquivo achada por `get_latest_update` (`extra_download_params`).

    Returns:
        `ExtractAndLoad` com a coverage da tabela e o diretório particionado
        (`ano=/trimestre=/sigla_uf=`) escrito por `build_partitions`.
    """
    url = download_params["url"]

    input_dir, output_dir = build_table_paths(table_id=MICRODADOS_TABLE_ID)
    # pyrefly: ignore [no-matching-overload]
    download_async(url, input_dir, "zip")
    build_partitions(input_dir, output_dir)

    return ExtractAndLoad(
        coverage=MICRODADOS_COVERAGE.model_dump(),
        data_path=str(output_dir),
        dump_mode="append",
    )


# ──────────────────────────────────────────────────────────────────────────────
# dicionario (ainda não migrado)
# ──────────────────────────────────────────────────────────────────────────────


@task
def build_dicionario_task(work_dir: str, output_dir: str) -> str:
    """Monta o dicionário e escreve um parquet all-string para upload.

    Args:
        work_dir: Diretório de trabalho para baixar a documentação do IBGE.
        output_dir: Diretório onde escrever o parquet a ser enviado ao staging.

    Returns:
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
