"""Publish the cleaned Censo 2022 extracts and the IBGE documentation.

Two destinations, both reached through the repo's own helpers:

- staging data → ``gs://basedosdados-dev/staging/<dataset>/<table>/`` plus the
  EXTERNAL BigQuery table, via ``_upload_to_gcs``;
- one auxiliary-file bundle per table →
  ``gs://basedosdados/auxiliary_files/<dataset>/<table>/auxiliary_files.zip``.

Prod table data is never written from here — the table-approve action
materialises ``basedosdados.<dataset>.*`` when the onboarding PR merges.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
      uv run python models/br_ibge_censo_demografico/code/sync_gcs.py \
        [--skip-staging] [--skip-aux] [--delete-local]
"""

from __future__ import annotations

import argparse
import csv
import shutil
import zipfile
from datetime import date
from pathlib import Path

import pyarrow.dataset as pads
from google.cloud import bigquery, storage

from models.br_ibge_censo_demografico.code import constants
from pipelines.utils.tasks import _upload_to_gcs

# Local trees that are reproducible and should be dropped after a verified sync.
SYNC_DIRS = ("output", "docs")
SYNC_FILES = ("row_counts.json",)

# The dicionario is shared with the 1970-2010 tables: its staging prefix holds
# the historical CSV alongside the 2022 one, and the model reads both.
DICIONARIO_HISTORICAL = "dicionario.csv"

TABLE_SLUGS = [spec["slug"] for spec in constants.TABLES.values()]
ALL_SLUGS = [*TABLE_SLUGS, "dicionario"]


def assert_dev_target() -> None:
    """Refuse to run against anything but the dev lake.

    ``_upload_to_gcs`` recreates the staging table, which deletes the existing
    one; pointed at prod that would drop a published table.
    """
    if constants.STAGING_BUCKET != "basedosdados-dev":
        raise RuntimeError(
            f"staging bucket is {constants.STAGING_BUCKET!r}; this script only "
            "writes basedosdados-dev. Prod tables come from table-approve."
        )


def fetch_historical_dicionario(client: storage.Client) -> Path:
    """Put the 1970-2010 dicionario CSV next to the 2022 one, locally.

    ``_upload_to_gcs`` clears the table's staging prefix and re-uploads only
    what is on disk, so uploading the 2022 file alone would silently drop the
    historical rows from the external table.

    Args:
        client: A GCS client with read access to the dev staging bucket.

    Returns:
        Path of the local historical CSV.

    Raises:
        FileNotFoundError: If the historical CSV is absent from the bucket, in
            which case uploading would truncate the dictionary.
    """
    dest = constants.OUTPUT_DIR / "dicionario" / DICIONARIO_HISTORICAL
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  historical dicionario already local: {dest}", flush=True)
        return dest
    blob_name = (
        f"staging/{constants.DATASET_ID}/dicionario/{DICIONARIO_HISTORICAL}"
    )
    # Both lake buckets are requester-pays, so every request needs a billing
    # project of its own — the client's project is not enough.
    bucket = client.bucket(
        constants.STAGING_BUCKET, user_project=constants.STAGING_BUCKET
    )
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(
            f"gs://{constants.STAGING_BUCKET}/{blob_name} not found. Uploading "
            "only the 2022 file would drop the 1970-2010 dictionary rows; "
            "restore the historical CSV before syncing."
        )
    dest.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(dest))
    print(
        f"  fetched gs://{constants.STAGING_BUCKET}/{blob_name} → {dest}",
        flush=True,
    )
    return dest


def local_rows(slug: str) -> int:
    """Count the rows the local extract holds for one table."""
    root = constants.OUTPUT_DIR / slug
    if slug == "dicionario":
        total = 0
        for path in sorted(root.glob("*.csv")):
            with path.open(encoding="utf-8", newline="") as handle:
                total += max(sum(1 for _ in csv.reader(handle)) - 1, 0)
        return total
    return pads.dataset(
        str(root), format="parquet", partitioning="hive"
    ).count_rows()


def staging_rows(client: bigquery.Client, slug: str) -> int:
    """Count the rows the staging external table exposes for one table."""
    dest = f"{constants.GCP_PROJECT}.{constants.DATASET_ID}_staging.{slug}"
    query = f"select count(*) as n from `{dest}`"
    return next(iter(client.query(query).result())).n


def upload_staging(slugs: list[str]) -> None:
    """Upload each table's extract and assert local↔BigQuery row parity."""
    assert_dev_target()
    storage_client = storage.Client(project=constants.GCP_PROJECT)
    bq_client = bigquery.Client(project=constants.GCP_PROJECT)
    for slug in slugs:
        print(f"=== {slug} ===", flush=True)
        path = constants.OUTPUT_DIR / slug
        if not path.exists():
            raise FileNotFoundError(f"missing {path}; run clean.py")
        if slug == "dicionario":
            fetch_historical_dicionario(storage_client)
        expected = local_rows(slug)
        _upload_to_gcs(
            path,
            dataset_id=constants.DATASET_ID,
            table_id=slug,
            bucket_name=constants.STAGING_BUCKET,
            # Rebuild the prefix and the table from what is on disk, so a
            # rerun is deterministic and a stale native table is replaced.
            dump_mode="overwrite",
            source_format="csv" if slug == "dicionario" else "parquet",
        )
        found = staging_rows(bq_client, slug)
        if found != expected:
            raise ValueError(
                f"{slug}: staging has {found:,} rows, local has {expected:,}"
            )
        print(f"  {slug}: {found:,} rows — OK", flush=True)


def bundle_readme(slug: str) -> str:
    """Build the README that ships inside one table's bundle."""
    today = date.today().isoformat()
    lines = [
        f"# Arquivos auxiliares — `{constants.DATASET_ID}.{slug}`",
        "",
        "## Citação",
        "",
        constants.CITATION,
        "",
        "## Arquivos neste pacote",
        "",
    ]
    for bundle_name, (_local, url) in constants.DOC_FILES.items():
        lines += [
            f"- `{bundle_name}`",
            f"  - origem: {url}",
            f"  - baixado em: {today}",
        ]
    lines += [
        "",
        "O layout descreve a posição, o tipo e os valores válidos de cada",
        "variável do arquivo de acesso público. O dicionário de variáveis traz",
        "os rótulos dos códigos — os mesmos registrados na tabela",
        "`dicionario` deste conjunto.",
        "",
        "## Documentos mantidos na fonte",
        "",
    ]
    for title, url in constants.DOC_LINKS.items():
        lines.append(f"- {title}: {url}")
    lines += [
        "",
        "## Observações sobre a tabela",
        "",
        "- Os nomes das colunas seguem os códigos do IBGE (`P0150`, `D0130`)",
        "  quando não há um nome consagrado, mantendo a compatibilidade com as",
        "  tabelas de 1970 a 2010 deste mesmo conjunto. O nome original de cada",
        "  coluna está registrado nos metadados.",
        "- `ano` e `sigla_uf` são colunas de partição: no arquivo bruto a UF vem",
        "  como código de dois dígitos e foi convertida para a sigla.",
        "- `peso_amostral` é adimensional e precisa ser usado em qualquer",
        "  estimativa a partir da amostra.",
        "- O arquivo de acesso público traz apenas registros com risco de",
        "  revelação abaixo de 20%, idade em grupos quinquenais e sem as",
        "  variáveis quase-identificadoras; a geografia máxima é a UF.",
        "- Campos vazios na origem foram carregados como nulos.",
        "",
    ]
    return "\n".join(lines)


def build_bundle(slug: str) -> Path:
    """Zip one table's documentation, with a README at the top.

    Args:
        slug: The table slug the bundle belongs to.

    Returns:
        Path of the written ``auxiliary_files.zip``.

    Raises:
        FileNotFoundError: If a document listed in ``DOC_FILES`` was not
            downloaded, so the bundle would ship incomplete.
    """
    out_dir = constants.DATA_ROOT / "auxiliary_files" / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / "auxiliary_files.zip"
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("README.md", bundle_readme(slug))
        for bundle_name, (local_name, _url) in constants.DOC_FILES.items():
            source = constants.DOCS_DIR / local_name
            if not source.exists():
                raise FileNotFoundError(
                    f"missing {source}; run download.py before bundling"
                )
            zf.write(source, bundle_name)
    print(
        f"  {slug}: bundle {dest.stat().st_size / 1e6:.1f} MB → {dest}",
        flush=True,
    )
    return dest


def upload_bundles(slugs: list[str]) -> None:
    """Build and upload one auxiliary-file bundle per table."""
    client = storage.Client(project=constants.AUX_BUCKET)
    bucket = client.bucket(
        constants.AUX_BUCKET, user_project=constants.AUX_BUCKET
    )
    for slug in slugs:
        print(f"=== {slug} (auxiliary files) ===", flush=True)
        local = build_bundle(slug)
        blob_name = f"{constants.AUX_PREFIX}/{slug}/auxiliary_files.zip"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(local))
        blob.reload()
        if blob.size != local.stat().st_size:
            raise RuntimeError(
                f"{slug}: uploaded {blob.size} bytes, local has "
                f"{local.stat().st_size}"
            )
        print(f"  gs://{constants.AUX_BUCKET}/{blob_name}", flush=True)
        print(
            f"  register as: {constants.auxiliary_files_url(slug)}", flush=True
        )


def delete_local() -> None:
    """Remove the reproducible local scratch after a verified sync."""
    for dirname in SYNC_DIRS:
        path = constants.DATA_ROOT / dirname
        if path.exists():
            shutil.rmtree(path)
            print(f"deleted {path}")
    for name in SYNC_FILES:
        (constants.DATA_ROOT / name).unlink(missing_ok=True)
    for path in (constants.INPUT_DIR, constants.DATA_ROOT / "dbt"):
        if path.exists():
            shutil.rmtree(path)
            print(f"deleted {path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tables",
        default="",
        help=f"Comma-separated slugs; default all ({', '.join(ALL_SLUGS)})",
    )
    parser.add_argument(
        "--skip-staging", action="store_true", help="Do not touch the lake"
    )
    parser.add_argument(
        "--skip-aux",
        action="store_true",
        help="Do not build or upload the auxiliary-file bundles",
    )
    parser.add_argument(
        "--delete-local",
        action="store_true",
        help="Remove local scratch after a verified sync",
    )
    args = parser.parse_args()
    wanted = {t.strip() for t in args.tables.split(",") if t.strip()}
    unknown = wanted - set(ALL_SLUGS)
    if unknown:
        parser.error(f"unknown tables: {sorted(unknown)}")
    slugs = [s for s in ALL_SLUGS if not wanted or s in wanted]

    if not args.skip_staging:
        upload_staging(slugs)
    if not args.skip_aux:
        # The dicionario has no documentation of its own; the codebook belongs
        # to the tables it describes.
        upload_bundles([s for s in slugs if s != "dicionario"])
    if args.delete_local:
        delete_local()
        print("local extracts removed")


if __name__ == "__main__":
    main()
