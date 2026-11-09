"""Carga inicial de br_senado_dados_abertos_administrativos.

Extrai e limpa as 40 tabelas com `clean_all`, de `utils.py`, e grava o parquet
em `tmp/br_senado_dados_abertos_administrativos/output/`, na raiz do
repositório. Com `--upload`, sobe cada tabela para `basedosdados-dev` com
`upload_to_gcs`, a mesma task do pipeline, em modo `overwrite`.

Uso:
    uv run pipelines/datasets/br_senado_dados_abertos_administrativos/run_onboarding.py
    uv run pipelines/datasets/br_senado_dados_abertos_administrativos/run_onboarding.py \\
        --full
    uv run pipelines/datasets/br_senado_dados_abertos_administrativos/run_onboarding.py \\
        --full --upload
    uv run pipelines/datasets/br_senado_dados_abertos_administrativos/run_onboarding.py \\
        --full --upload --skip-extract --tables despesa_ceaps servidor

Sem `--full`, a extração é uma amostra: as séries só do ano corrente e sem as
tabelas filhas das contratações. O upload exige `--full`, para uma amostra não
substituir o histórico na staging.
"""

import argparse
import datetime as dt
import glob
import os
import sys
from pathlib import Path

import pyarrow.parquet as pq

from pipelines.datasets.br_senado_dados_abertos_administrativos.constants import (
    constants,
)
from pipelines.datasets.br_senado_dados_abertos_administrativos.utils import (
    clean_all,
)
from pipelines.utils.tasks import upload_to_gcs

OUTPUT_DIR = (
    Path(__file__).resolve().parents[3]
    / "tmp"
    / constants.DATASET_ID.value
    / "output"
)


def count_rows(table_dir: Path) -> int:
    """Conta as linhas de todos os parquet de uma tabela.

    Args:
        table_dir: diretório da tabela.

    Returns:
        O total de linhas.
    """
    files = glob.glob(
        os.path.join(table_dir, "**", "*.parquet"), recursive=True
    )
    return sum(pq.read_metadata(file).num_rows for file in files)


def extract(full: bool) -> None:
    """Extrai e limpa as tabelas e mostra as linhas de cada uma.

    Args:
        full: True para o histórico completo, com as tabelas filhas das
            contratações; False para a amostra.
    """
    years = None if full else [dt.date.today().year]
    counts = clean_all(str(OUTPUT_DIR), years=years, sub_resources=full)
    for table_id in constants.ALL_TABLES.value:
        if table_id not in counts:
            print(f"  {table_id:32} {'pulada':>10}")
            continue
        rows = count_rows(OUTPUT_DIR / table_id)
        print(f"  {table_id:32} {rows:>10,} linhas")


def upload(table_ids: list[str]) -> None:
    """Sobe as tabelas para a staging de `basedosdados-dev`.

    Args:
        table_ids: tabelas a subir, na ordem dada.

    Raises:
        FileNotFoundError: se uma tabela não tiver parquet em `OUTPUT_DIR`.
    """
    for table_id in table_ids:
        table_dir = OUTPUT_DIR / table_id
        rows = count_rows(table_dir) if table_dir.exists() else 0
        if rows == 0:
            raise FileNotFoundError(f"sem parquet em {table_dir}")
        print(f"=== {table_id}: {rows:,} linhas ===")
        upload_to_gcs.fn(
            data_path=str(table_dir),
            dataset_id=constants.DATASET_ID.value,
            table_id=table_id,
            bucket_name="basedosdados-dev",
            dump_mode="overwrite",
            source_format="parquet",
        )


def main() -> int:
    """Lê os argumentos e roda a extração e o upload pedidos.

    Returns:
        0 em caso de sucesso; 1 se alguma etapa falhar.
    """
    parser = argparse.ArgumentParser(
        description=f"Carga inicial de {constants.DATASET_ID.value}."
    )
    parser.add_argument(
        "--full", action="store_true", help="histórico completo"
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="sobe para a staging de basedosdados-dev; exige --full",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="não extrai; sobe o parquet já gravado em OUTPUT_DIR",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=constants.ALL_TABLES.value,
        choices=constants.ALL_TABLES.value,
        metavar="TABELA",
        help="tabelas a subir (padrão: todas)",
    )
    args = parser.parse_args()

    if args.upload and not args.full:
        parser.error("--upload exige --full")
    if args.skip_extract and not args.upload:
        parser.error("--skip-extract só faz sentido com --upload")

    print(f"output: {OUTPUT_DIR}")
    try:
        if not args.skip_extract:
            extract(full=args.full)
        if args.upload:
            upload(args.tables)
    except Exception as error:
        print(f"FALHOU: {type(error).__name__}: {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
