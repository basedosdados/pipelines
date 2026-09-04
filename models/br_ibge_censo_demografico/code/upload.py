"""Upload cleaned Censo 2022 parquet to sandbox-507414.

Never writes basedosdados-dev or basedosdados. Loads one hive partition at a
time and checks local↔BQ row-count parity.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/sandbox-507414.json \
      uv run python models/br_ibge_censo_demografico/code/upload.py [table_slug ...]
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq
from google.cloud import bigquery

from models.br_ibge_censo_demografico.code import constants

os.environ.setdefault(
    "GOOGLE_APPLICATION_CREDENTIALS",
    str(Path.home() / ".basedosdados" / "credentials" / "sandbox-507414.json"),
)

STAGING_DATASET = f"{constants.DATASET_ID}_staging"
# Smallest first so a bad table fails before the 21M-row person upload.
TABLES = [
    "dicionario",
    "microdados_mortalidade_2022",
    "microdados_familia_2022",
    "microdados_domicilio_2022",
    "microdados_pessoa_2022",
]


def local_rows(path: Path) -> int:
    return pads.dataset(
        str(path), format="parquet", partitioning="hive"
    ).count_rows()


def _string_table(table: pa.Table) -> pa.Table:
    arrays = []
    fields = []
    for name in table.column_names:
        values = table[name]
        if not pa.types.is_string(values.type):
            values = values.cast(pa.string())
        arrays.append(values)
        fields.append(pa.field(name, pa.string()))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def iter_partitions(slug: str):
    """Yield (index, total, table) per on-disk parquet, hive cols as STRING."""
    root = constants.OUTPUT_DIR / slug
    if slug == "dicionario":
        path = root / "data.parquet"
        if not path.exists():
            raise FileNotFoundError(path)
        yield 0, 1, _string_table(pq.read_table(path))
        return

    paths = sorted(root.rglob("data.parquet"))
    if not paths:
        raise FileNotFoundError(f"no parquet under {root}")
    total = len(paths)
    for index, parquet_path in enumerate(paths):
        table = pq.ParquetFile(parquet_path).read()
        parts = {
            p.split("=", 1)[0]: p.split("=", 1)[1]
            for p in parquet_path.parts
            if "=" in p
        }
        for hive_col in ("ano", "sigla_uf"):
            if hive_col in table.column_names:
                table = table.drop(hive_col)
            table = table.append_column(
                hive_col,
                pa.array([parts[hive_col]] * table.num_rows, type=pa.string()),
            )
        yield index, total, _string_table(table)


def ensure_dataset(client: bigquery.Client) -> None:
    dataset = bigquery.Dataset(f"{constants.GCP_PROJECT}.{STAGING_DATASET}")
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)


def _schema_of(table: pa.Table) -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField(name, "STRING") for name in table.column_names
    ]


def upload_table(client: bigquery.Client, slug: str) -> int:
    path = constants.OUTPUT_DIR / slug
    if not path.exists():
        raise FileNotFoundError(f"missing {path}")
    expected = local_rows(path)
    dest = f"{constants.GCP_PROJECT}.{STAGING_DATASET}.{slug}"
    client.delete_table(dest, not_found_ok=True)
    for _ in range(15):
        try:
            client.get_table(dest)
            time.sleep(1)
            client.delete_table(dest, not_found_ok=True)
        except Exception:
            break
    tmp = Path("/tmp") / f"{constants.DATASET_ID}_{slug}.parquet"
    writer: pq.ParquetWriter | None = None
    written = 0
    schema = None
    for index, total, table in iter_partitions(slug):
        if writer is None:
            schema = _schema_of(table)
            writer = pq.ParquetWriter(
                tmp,
                table.schema,
                compression="snappy",
                use_dictionary=False,
            )
        writer.write_table(table)
        written += table.num_rows
        print(
            f"    packed partition {index + 1}/{total} ({table.num_rows:,} rows)",
            flush=True,
        )
        del table
    if writer is None:
        raise FileNotFoundError(f"no partitions for {slug}")
    writer.close()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )
    last_exc: Exception | None = None
    job = None
    for attempt in range(3):
        try:
            with tmp.open("rb") as handle:
                job = client.load_table_from_file(
                    handle, dest, job_config=job_config
                )
            job.result()
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            print(
                f"    retry {attempt + 1}/3 after {type(exc).__name__}: {exc}",
                flush=True,
            )
            time.sleep(2**attempt)
    if last_exc is not None:
        raise last_exc
    print(f"    load job output_rows={job.output_rows:,}", flush=True)
    tmp.unlink(missing_ok=True)
    n = next(
        iter(client.query(f"select count(*) as n from `{dest}`").result())
    ).n
    status = (
        "OK"
        if n == expected
        else f"MISMATCH local={expected:,} packed={written:,}"
    )
    print(f"  {slug}: uploaded {n:,} rows — {status}", flush=True)
    if n != expected:
        raise ValueError(f"{slug}: BQ {n:,} != local {expected:,}")
    return n


def main() -> None:
    only = set(sys.argv[1:])
    tables = [s for s in TABLES if not only or s in only]
    unknown = only - set(TABLES)
    if unknown:
        print(f"unknown tables: {sorted(unknown)}\nvalid: {TABLES}")
        sys.exit(2)
    client = bigquery.Client(project=constants.GCP_PROJECT)
    print(
        f"=== upload to {constants.GCP_PROJECT}.{STAGING_DATASET} ===",
        flush=True,
    )
    ensure_dataset(client)
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(client, slug)
        except Exception as exc:
            print(f"  FAILED: {type(exc).__name__}: {exc}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
