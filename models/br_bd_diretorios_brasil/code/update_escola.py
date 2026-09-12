"""One-shot bootstrap: download + clean + (optionally) upload escola directory.

Usage
-----
    # só download + limpeza (padrão)
    uv run python models/br_bd_diretorios_brasil/code/update_escola.py

    # com upload para BigQuery dev
    uv run python models/br_bd_diretorios_brasil/code/update_escola.py --upload

    # especifica diretório de input/output
    uv run python models/br_bd_diretorios_brasil/code/update_escola.py \
        --input  /caminho/input \
        --output /caminho/output

Lookup de id_municipio
-----------------------
Por padrão lê ``br_bd_diretorios_brasil.municipio`` direto do BigQuery
(via basedosdados). Opcionalmente aceita um CSV local com
``--municipio-csv``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("update_escola")

# Paths relative to the repo root (where uv run is executed from)
_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_INPUT = _REPO_ROOT / "input" / "br_bd_diretorios_brasil"
_DEFAULT_OUTPUT = _REPO_ROOT / "output" / "br_bd_diretorios_brasil"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, default=_DEFAULT_INPUT)
    p.add_argument("--output", type=Path, default=_DEFAULT_OUTPUT)
    p.add_argument(
        "--skip-download",
        action="store_true",
        help="reutiliza o CSV já baixado em --input",
    )
    p.add_argument(
        "--municipio-csv",
        type=Path,
        default=None,
        help="CSV local opcional; se omitido, lê municipio do BigQuery",
    )
    p.add_argument(
        "--billing-project",
        default="basedosdados-dev",
        help="projeto GCP para billing da query ao municipio (default: basedosdados-dev)",
    )
    p.add_argument(
        "--upload",
        action="store_true",
        help="faz upload do parquet para BigQuery dev após a limpeza",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    from pipelines.datasets.br_bd_diretorios_brasil.utils import (
        build_municipio_lookup,
        build_municipio_lookup_from_bq,
        clean_catalogo,
        download_catalogo,
    )

    # 1. Download
    csv_path = args.input / "catalogo_escolas.csv"
    if args.skip_download and csv_path.exists():
        log.info("--skip-download: reusing %s", csv_path)
    else:
        csv_path = download_catalogo(args.input)

    # 2. Municipio lookup (BQ por padrão)
    if args.municipio_csv is not None:
        log.info("Loading municipio lookup from CSV %s", args.municipio_csv)
        lookup = build_municipio_lookup(args.municipio_csv)
    else:
        lookup = build_municipio_lookup_from_bq(
            billing_project_id=args.billing_project
        )

    # 3. Clean → parquet
    parquet_path = clean_catalogo(
        csv_path, args.output, municipio_lookup=lookup
    )

    # 4. Optional upload
    if args.upload:
        _upload(parquet_path)
    else:
        log.info(
            "Skipping upload. Run with --upload to push to BigQuery dev, or:\n"
            "  uv run python models/br_bd_diretorios_brasil/code/update_escola.py --upload"
        )


def _upload(parquet_path: Path) -> None:
    try:
        import basedosdados as bd
    except ImportError:
        log.error("basedosdados not installed. Run: pip install basedosdados")
        sys.exit(1)

    log.info(
        "Uploading to BigQuery "
        "(basedosdados-dev.br_bd_diretorios_brasil_staging.escola)..."
    )
    # Limpa CSV antigo no GCS (senão BQ tenta ler escola.csv como parquet).
    # API: create(path=..., if_table_exists=...) — não existe if_exists=.
    st = bd.Storage(
        dataset_id="br_bd_diretorios_brasil",
        table_id="escola",
    )
    st.delete_table(mode="staging", not_found_ok=True)

    tb = bd.Table(
        table_id="escola",
        dataset_id="br_bd_diretorios_brasil",
    )
    tb.create(
        path=parquet_path,
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )
    log.info("Upload complete.")


if __name__ == "__main__":
    main()
