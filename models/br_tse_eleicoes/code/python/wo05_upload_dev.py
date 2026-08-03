"""Work order 05 — upload the rebuilt partitioned CSVs to dev staging.

Uploads each of the 19 tables the Python pipeline produces to
``basedosdados-dev.br_tse_eleicoes_staging.<table>`` via the basedosdados
package (``bd.Table.create`` with a Hive-partitioned CSV directory). The three
dbt models the pipeline does not build (``receitas_comite``,
``receitas_orgao_partidario``, ``dicionario``) are out of refactor scope and
keep their existing dev staging tables.

Resumable: a table whose ``upload_done/<table>`` marker exists is skipped.
Sequential — one table at a time. Dev only; never targets prod here.

Run from ``code/python`` with the same WORK as the rebuild::

    TSE_WORK=/path/to/work uv run --with basedosdados python wo05_upload_dev.py \
        [--only t1,t2] [--skip t3,t4]
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

WORK = Path(
    os.environ.get("TSE_WORK", "/Users/rdahis/Downloads/dados_TSE_rebuild")
)
OUTPUT = WORK / "output_python"
MARKERS = WORK / "upload_done"

DATASET = "br_tse_eleicoes"
SA_KEY = "/Users/rdahis/.basedosdados/credentials/staging.json"
BUCKET = "basedosdados-dev"  # requester-pays; needs user_project on every op

# The three giant seção families are partitioned as parquet (not CSV) — as
# all-string CSV a single one exceeds 50 GB. Uploaded with source_format
# parquet; every other table is CSV.
GIANT_PARQUET = {
    "resultados_candidato_secao",
    "resultados_partido_secao",
    "perfil_eleitorado_secao",
}

# The 19 tables the Python pipeline produces, in dependency order (phase-1/2
# first, aggregations last). Each is a Hive-partitioned CSV dir under OUTPUT.
TABLES = [
    "candidatos",
    "partidos",
    "vagas",
    "bens_candidato",
    "receitas_candidato",
    "despesas_candidato",
    "perfil_eleitorado_municipio_zona",
    "perfil_eleitorado_secao",
    "perfil_eleitorado_local_votacao",
    "detalhes_votacao_municipio_zona",
    "detalhes_votacao_secao",
    "detalhes_votacao_municipio",
    "resultados_candidato_municipio_zona",
    "resultados_partido_municipio_zona",
    "resultados_candidato_secao",
    "resultados_partido_secao",
    "resultados_candidato_municipio",
    "resultados_partido_municipio",
    "resultados_candidato",
]


def _n_partitions(path: Path, ext: str) -> int:
    return sum(1 for _ in path.rglob(f"*.{ext}"))


def _clean_staging_prefix(table: str) -> None:
    """Delete the GCS staging prefix before upload.

    bd's ``if_storage_data_exists="replace"`` overwrites same-named blobs but
    does NOT remove files a prior load left under other names — notably the
    ``.csv`` partitions from the original load, which make a parquet external
    table fail (``Input file is not in Parquet format``). Requester-pays bucket
    → every op carries ``user_project``.
    """
    from google.cloud import storage
    from google.oauth2 import service_account

    creds = service_account.Credentials.from_service_account_file(SA_KEY)
    client = storage.Client(project=BUCKET, credentials=creds)
    bucket = client.bucket(BUCKET, user_project=BUCKET)
    prefix = f"staging/{DATASET}/{table}/"
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    if blobs:
        bucket.delete_blobs(blobs)
        print(
            f"  [{table}] cleaned {len(blobs)} stale staging blobs", flush=True
        )


def upload_table(table: str) -> None:
    import basedosdados as bd

    fmt = "parquet" if table in GIANT_PARQUET else "csv"
    path = OUTPUT / table
    if not path.exists():
        print(f"  [{table}] MISSING dir {path} — SKIP", flush=True)
        return
    n = _n_partitions(path, fmt)
    if n == 0:
        print(f"  [{table}] 0 {fmt} partitions in {path} — SKIP", flush=True)
        return
    _clean_staging_prefix(table)
    print(
        f"  [{table}] uploading {n} {fmt} partitions from {path}", flush=True
    )
    tb = bd.Table(dataset_id=DATASET, table_id=table)
    tb.create(
        path=str(path),
        if_table_exists="replace",
        if_storage_data_exists="replace",
        source_format=fmt,
    )
    MARKERS.mkdir(parents=True, exist_ok=True)
    (MARKERS / table).write_text(f"{n} partitions\n")
    print(f"  [{table}] DONE ({n} partitions)", flush=True)


def main() -> None:
    only = None
    skip: set[str] = set()
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    if "--skip" in sys.argv:
        skip = set(sys.argv[sys.argv.index("--skip") + 1].split(","))
    print(f"WORK={WORK}", flush=True)
    for table in TABLES:
        if only and table not in only:
            continue
        if table in skip:
            print(f"  [{table}] in --skip — SKIP", flush=True)
            continue
        if (MARKERS / table).exists():
            print(
                f"  [{table}] marker present — SKIP (already uploaded)",
                flush=True,
            )
            continue
        upload_table(table)
    print("DEV UPLOAD COMPLETE", flush=True)


if __name__ == "__main__":
    main()
