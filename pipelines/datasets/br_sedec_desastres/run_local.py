#!/usr/bin/env python3
"""Roda as etapas do flow br_sedec_desastres localmente, sem o Prefect.

Chama exatamente as mesmas funções que o flow chama — as `@task` de
`pipelines/datasets/br_sedec_desastres/tasks.py` e as compartilhadas de
`pipelines/utils/tasks.py` — via `.fn()`, que executa a função por baixo da task
sem precisar do runtime do Prefect. Não há reimplementação aqui: se este script
funciona, é o mesmo código que vai rodar no worker.

Este script **é** o mecanismo de atualização da base, não um rascunho de
desenvolvimento: o flow não roda no cluster porque o S2ID barra o IP de saída do
pod por geolocalização. A receita mensal completa está no README do diretório.

O upload é fixo em `basedosdados-dev` — e é assim que prod é alimentado: no
merge, a action table-approve espelha esse prefixo para o bucket `basedosdados`
e materializa a tabela. Logo o prefixo de dev **é** o histórico da série;
apagá-lo apaga o dado em prod no merge seguinte.

O estágio `metadata` escreve no backend de **produção** e roda **depois** do
merge, porque ele lê a data máxima da tabela já materializada em prod.

O que continua fora: a fiação do próprio `flows.py` — a ordem das etapas, o
guarda do poll, os returns antecipados. Como o flow não roda em lugar nenhum
hoje, nada exercita isso.

Uso:
    # só baixa e limpa (padrão; não toca em nuvem nenhuma)
    uv run python pipelines/datasets/br_sedec_desastres/run_local.py

    # etapas específicas
    uv run python pipelines/datasets/br_sedec_desastres/run_local.py --stages download

    # o retrato do mês: baixa, limpa e sobe para o staging de dev
    uv run python pipelines/datasets/br_sedec_desastres/run_local.py \\
        --stages download,clean,upload

    # depois do merge da PR: atualiza cobertura, update e poll em prod
    uv run python pipelines/datasets/br_sedec_desastres/run_local.py --stages metadata
"""

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("run_flow_local")

DATASET_ID = "br_sedec_desastres"
TABLE_ID = "reconhecimentos_vigentes"

# `tmp/` na raiz já está no .gitignore do repo, então input/ e output/ ficam fora
# do versionamento — e persistem para inspeção, ao contrário do
# tempfile.mkdtemp() que o flow usa e apaga no finally. São três níveis até a
# raiz: br_sedec_desastres/ → datasets/ → pipelines/ → repo.
DEFAULT_WORK_DIR = Path(__file__).resolve().parents[3] / "tmp" / DATASET_ID

ALL_STAGES = ["download", "clean", "upload", "dbt", "metadata"]
CLOUD_STAGES = {"upload", "dbt"}
DATE_FORMAT = "%Y-%m-%d"


def check_flow_importable() -> None:
    """Avisa se o ``flows.py`` importa, sem derrubar a execução.

    O módulo do flow puxa ``pipelines.utils.metadata``, que precisa de Python
    3.11 por causa do ``enum.StrEnum``. Numa venv 3.10 esse import falha para
    todos os flows do repo, então este script chama as tasks direto. Vale
    imprimir: é a diferença entre "a transformação funciona" e "o flow
    funciona".
    """
    try:
        from pipelines.datasets.br_sedec_desastres import flows

        log.info(
            f"flows.py importa OK — flow: {flows.br_sedec_desastres_flow.name}"
        )
    except ImportError as e:
        log.warning(f"flows.py NÃO importa nesta venv: {e}")
        log.warning(
            "Esperado em Python < 3.11 (enum.StrEnum). Afeta todos os flows do "
            "repo, não só este. As etapas abaixo rodam de todo jeito."
        )


def stage_download(work_dir: Path) -> Path:
    """Roda a task de download e devolve o diretório de entrada."""
    from pipelines.datasets.br_sedec_desastres.tasks import (
        download_reconhecimentos,
    )

    log.info("=== download ===")
    input_dir = Path(download_reconhecimentos.fn(work_dir=str(work_dir)))
    files = sorted(p.name for p in input_dir.iterdir() if p.is_file())
    log.info(f"{len(files)} arquivo(s) em {input_dir}: {files}")
    return input_dir


def stage_clean(work_dir: Path) -> dict:
    """Roda a task de limpeza e informa o que ela produziu."""
    from pipelines.datasets.br_sedec_desastres.tasks import (
        clean_reconhecimentos,
    )

    log.info("=== clean ===")
    input_dir = work_dir / "input"
    if not input_dir.exists():
        raise FileNotFoundError(
            f"{input_dir} não existe — rode a etapa download primeiro"
        )

    result = clean_reconhecimentos.fn(
        work_dir=str(work_dir), input_dir=str(input_dir)
    )
    log.info(f"max_date devolvido ao poll: {result.get('max_date')!r}")

    # Contagem por tabela: é ela que vira o expected_rows do upload.py.
    import pandas as pd

    for table in [TABLE_ID]:
        tdir = Path(result[table])
        parts = sorted(tdir.rglob("*.parquet"))
        rows = sum(len(pd.read_parquet(p)) for p in parts)
        log.info(f"{table}: {rows:,} linhas em {len(parts)} partição(ões)")
        if parts:
            df = pd.read_parquet(parts[0])
            log.info(f"{table}: colunas = {list(df.columns)}")
            log.info(f"{table}: primeira partição =\n{df.head(3)}")
    return result


def stage_upload(result: dict) -> None:
    """Sobe o parquet limpo para o bucket de staging de dev."""
    from pipelines.utils.tasks import upload_to_gcs

    log.info("=== upload (dev) ===")
    upload_to_gcs.fn(
        data_path=result[TABLE_ID],
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name="basedosdados-dev",
        # "append" acompanha a decisão do flow: acumular um retrato por execução.
        dump_mode="append",
        source_format="parquet",
    )
    log.info("upload concluído")


def stage_dbt() -> None:
    """Roda dbt run + test no target de dev."""
    from pipelines.utils.tasks import run_dbt

    log.info("=== dbt (dev) ===")
    run_dbt.fn(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dbt_command="run/test",
        target="dev",
    )
    log.info("dbt concluído")


def _max_partition_date(work_dir: Path) -> str:
    """Lê a data do retrato mais recente pelos nomes das partições em output/.

    Args:
        work_dir: Diretório de trabalho da execução.

    Returns:
        A maior ``data_extracao`` encontrada, no formato ``AAAA-MM-DD``.

    Raises:
        FileNotFoundError: Se não houver nenhuma partição no diretório.
    """
    output_dir = work_dir / "output" / TABLE_ID
    dates = sorted(
        p.name.split("=", 1)[1]
        for p in output_dir.glob("data_extracao=*")
        if p.is_dir()
    )
    if not dates:
        raise FileNotFoundError(
            f"nenhuma partição em {output_dir} — rode a etapa clean antes, ou "
            "passe --max-date"
        )
    return dates[-1]


def stage_metadata(work_dir: Path, max_date: str | None) -> None:
    """Escreve os três registros de metadado no backend de produção.

    Roda depois do merge da PR: o ``register_table_materialization_task`` lê a
    data máxima em ``basedosdados.br_sedec_desastres.reconhecimentos_vigentes``,
    que só existe depois de a action table-approve materializar.

    Args:
        work_dir: Diretório de trabalho, usado para descobrir a data do retrato
            quando ``max_date`` não é informada.
        max_date: Data do retrato promovido, no formato ``AAAA-MM-DD``. Quando
            ``None``, vem da maior partição em ``output/``.
    """
    # A spec de cobertura vem do flow, para não existirem duas definições dela.
    from pipelines.datasets.br_sedec_desastres.flows import _COVERAGE
    from pipelines.utils.metadata.tasks import (
        commit_source_update_task,
        poll_source_for_update_task,
        register_table_materialization_task,
    )

    max_date = max_date or _max_partition_date(work_dir)
    log.info(f"=== metadata (backend de PROD) — retrato {max_date} ===")

    # O poll existe só para registrar o Poll da fonte; o retorno é ignorado de
    # propósito, porque todo retrato mensal é legitimamente novo (decisão 4 do
    # README).
    poll_source_for_update_task.fn(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        source_max_date=max_date,
        env="prod",
        date_format=DATE_FORMAT,
    )
    register_table_materialization_task.fn(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        coverage=_COVERAGE,
        env="prod",
        bq_project="basedosdados",
    )
    commit_source_update_task.fn(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        source_max_date=max_date,
        env="prod",
        date_format=DATE_FORMAT,
    )
    log.info("metadata concluído")


def main() -> int:
    """Lê os argumentos, roda as etapas pedidas e para na primeira falha."""
    parser = argparse.ArgumentParser(
        description=f"Roda as etapas do flow {DATASET_ID} localmente."
    )
    parser.add_argument(
        "--stages",
        default="download,clean",
        help=(
            "lista separada por vírgula: "
            f"{','.join(ALL_STAGES)} (padrão: download,clean)"
        ),
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=DEFAULT_WORK_DIR,
        help=f"diretório de trabalho (padrão: {DEFAULT_WORK_DIR})",
    )
    parser.add_argument(
        "--max-date",
        default=None,
        help=(
            "data do retrato para a etapa metadata, em AAAA-MM-DD (padrão: a "
            "maior partição em output/)"
        ),
    )
    args = parser.parse_args()

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    unknown = [s for s in stages if s not in ALL_STAGES]
    if unknown:
        parser.error(f"etapa(s) desconhecida(s): {unknown}; use {ALL_STAGES}")

    work_dir = args.work_dir
    work_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"dataset: {DATASET_ID}.{TABLE_ID}")
    log.info(f"work_dir: {work_dir}")
    log.info(f"etapas: {stages}")
    if CLOUD_STAGES & set(stages):
        log.info(
            "etapas de nuvem incluídas — escrevem em basedosdados-dev, que é o "
            "prefixo que a table-approve promove para prod no merge"
        )
    if "metadata" in stages:
        log.warning(
            "a etapa metadata escreve no backend de PRODUÇÃO e só faz sentido "
            "depois do merge da PR — ela lê a tabela já materializada em prod"
        )
    check_flow_importable()

    result: dict = {}
    try:
        if "download" in stages:
            stage_download(work_dir)
        if "clean" in stages:
            result = stage_clean(work_dir)
        if "upload" in stages:
            if not result:
                raise RuntimeError(
                    "upload exige a etapa clean na mesma execução (ela devolve "
                    "os caminhos das partições)"
                )
            stage_upload(result)
        if "dbt" in stages:
            stage_dbt()
        if "metadata" in stages:
            stage_metadata(work_dir, args.max_date)
    except NotImplementedError:
        log.error(
            "NotImplementedError — a etapa ainda é esqueleto. Ver os TODO em "
            "pipelines/datasets/br_sedec_desastres/utils.py"
        )
        return 1
    except Exception as e:
        log.error(f"FALHOU: {type(e).__name__}: {e}")
        return 1

    log.info("=== OK ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
