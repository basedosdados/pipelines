#!/usr/bin/env python3
"""Carga inicial da tabela reconhecimentos_realizados, um ano por vez.

Chama as mesmas tasks do flow via `.fn()` e grava em
`tmp/br_sedec_desastres/reconhecimentos_realizados/`. Ano com partição já gravada
é pulado.

Uso:
    # baixa e limpa de 2013 a 2025
    uv run pipelines/datasets/br_sedec_desastres/run_local_reconhecimentos_realizados.py

    # outro intervalo, e sobe para o staging de dev no fim
    uv run pipelines/datasets/br_sedec_desastres/run_local_reconhecimentos_realizados.py \\
        --de 2013 --ate 2025 --upload
"""

import argparse
import logging
import sys
from pathlib import Path

from pipelines.datasets.br_sedec_desastres.constants import (
    constants,
    reconhecimentos_realizados,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("run_local_reconhecimentos_realizados")

DATASET_ID = constants.DATASET_ID.value
TABLE_ID = reconhecimentos_realizados.TABLE_ID.value

DEFAULT_WORK_DIR = (
    Path(__file__).resolve().parents[3] / "tmp" / DATASET_ID / TABLE_ID
)


def load_year(year: int, work_dir: Path) -> None:
    """Baixa e limpa um ano, gravando a partição ``ano=<ano>``.

    Args:
        year: Ano a carregar.
        work_dir: Diretório de trabalho.
    """
    from pipelines.datasets.br_sedec_desastres.tasks import (
        clean_realizados,
        download_realizados,
    )

    input_dir = download_realizados.fn(work_dir=str(work_dir), years=[year])
    clean_realizados.fn(work_dir=str(work_dir), input_dir=input_dir)


def upload_to_dev(data_path: Path) -> None:
    """Sobe as partições gravadas para o staging de ``basedosdados-dev``.

    Args:
        data_path: Diretório da tabela, com as pastas ``ano=<ano>``.
    """
    from pipelines.utils.tasks import upload_to_gcs

    upload_to_gcs.fn(
        data_path=str(data_path),
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name="basedosdados-dev",
        dump_mode="append",
        source_format="parquet",
    )


def main() -> int:
    """Lê os argumentos, carrega os anos pedidos e para na primeira falha."""
    parser = argparse.ArgumentParser(
        description=f"Carga inicial de {DATASET_ID}.{TABLE_ID}."
    )
    parser.add_argument(
        "--de",
        dest="first_year",
        type=int,
        default=reconhecimentos_realizados.PRIMEIRO_ANO.value,
    )
    parser.add_argument("--ate", dest="last_year", type=int, default=2025)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()

    work_dir = args.work_dir
    data_path = work_dir / "output" / TABLE_ID
    work_dir.mkdir(parents=True, exist_ok=True)

    try:
        for year in range(args.first_year, args.last_year + 1):
            if (data_path / f"ano={year}" / "data.parquet").exists():
                log.info(f"ano={year}: já gravado, pulando")
                continue
            log.info(f"=== ano={year} ===")
            load_year(year, work_dir)

        if args.upload:
            log.info(f"=== upload de {data_path} para basedosdados-dev ===")
            upload_to_dev(data_path)
    except Exception as e:
        log.error(f"FALHOU: {type(e).__name__}: {e}")
        return 1

    log.info("=== OK ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
