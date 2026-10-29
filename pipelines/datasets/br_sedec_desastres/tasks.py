"""Tasks Prefect 3 do br_sedec_desastres — cascas finas sobre o utils.py.

`get_latest_update`/`extract_load_data`: funções simples (não `@task`, mesma
convenção de `br_bcb_taxa_selic/tasks.py`) que orquestram as tasks abaixo para
o par check_update/extract_and_load do staged pipeline — ver o banner no topo
de `flows.py` para por que `get_latest_update` baixa o retrato inteiro em vez
de um sinal barato de frescor.
"""

import shutil
import tempfile
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.br_sedec_desastres.constants import constants
from pipelines.datasets.br_sedec_desastres.utils import (
    clean_all,
    download_reconhecimentos_vigentes,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

DATASET_ID = constants.DATASET_ID.value
TABLE_ID = constants.TABLE_ID.value


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


def get_latest_update() -> SourceInspection:
    """Baixa e monta o retrato inteiro só para ler a `data_extracao` de hoje.

    A fonte não tem sinal de frescor mais barato que isso: "vigentes" é um
    retrato do momento, sem data de publicação própria — a única data
    disponível é a que o próprio retrato carimba
    (`utils.build_reconhecimentos_vigentes`, sempre `date.today()`), e ela só
    existe depois de baixar e montar os 27 exports por UF.
    `extract_load_data` baixa de novo, de forma independente (custo
    duplicado aceito; a variante `check_and_download` que reaproveitaria este
    download entre as duas etapas foi cancelada — ver o banner em `flows.py`).

    Returns:
        `SourceInspection` com a `data_extracao` do retrato como
        `reference_date`.
    """
    work_dir = tempfile.mkdtemp(prefix="br_sedec_desastres_check_")
    try:
        input_dir = download_reconhecimentos(work_dir=work_dir)
        result = clean_reconhecimentos(work_dir=work_dir, input_dir=input_dir)
        return SourceInspection(
            reference_date=date.fromisoformat(result["max_date"])
        )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Baixa de novo o retrato inteiro e devolve o diretório pronto pro upload.

    Args:
        download_params: recebido do check_update via `run_deployment()` —
            carrega `reference_date`, só para rastreabilidade. Não é usado
            para decidir o que baixar: a fonte não tem parâmetro de período,
            o retrato exportado é sempre o do dia corrente (não dá pra pedir
            um retrato passado).

    Returns:
        `ExtractAndLoad` com a coverage da tabela e o diretório particionado
        (`data_extracao=AAAA-MM-DD`) que `utils.write_partitioned` escreveu.
    """
    work_dir = tempfile.mkdtemp(prefix="br_sedec_desastres_extract_")
    input_dir = download_reconhecimentos(work_dir=work_dir)
    result = clean_reconhecimentos(work_dir=work_dir, input_dir=input_dir)

    # Só o diretório de entrada (os 27 CSVs brutos) é descartável aqui: o
    # particionado em `result[TABLE_ID]` precisa sobreviver até
    # `CheckThenExtractLoadPipeline.run_extract_and_load` terminar o
    # `upload_to_gcs`, que roda só depois desta função retornar.
    shutil.rmtree(input_dir, ignore_errors=True)

    return ExtractAndLoad(
        data_path=result[TABLE_ID],
        dump_mode="append",
        source_format="parquet",
    )
