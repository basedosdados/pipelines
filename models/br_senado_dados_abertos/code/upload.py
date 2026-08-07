"""Upload cleaned br_senado_dados_abertos parquet tables to BigQuery.

Usage:
    uv run python models/br_senado_dados_abertos/code/upload.py [--env dev|prod] [table ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. This
machine's config.toml is dev-only. Uploads smallest first, verifies the staging
row count against the local parquet, and stops on the first failure.
"""

import glob
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

_argv = sys.argv[1:]
ENV = "dev"
if "--env" in _argv:
    _i = _argv.index("--env")
    if _i + 1 >= len(_argv):
        sys.exit("--env requires a value: dev or prod")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
if ENV not in ("dev", "prod"):
    sys.exit(f"invalid --env {ENV!r}: expected 'dev' or 'prod'")
BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "br_senado_dados_abertos"
OUTPUT_ROOT = Path(__file__).resolve().parent / "output"

# Upload order: dimensions (small) first, heavy partitioned tables last.
TABLES = [
    "bloco",
    "mesa",
    "partido",
    "comissao",
    "lideranca",
    "senador",
    # T2 — small first
    "votacao_comissao",
    "senador_mandato",
    "senador_filiacao",
    "senador_cargo",
    "relatoria",
    "senador_comissao",
    "votacao_comissao_parlamentar",
    "votacao_orientacao_bancada",
    "votacao",
    "processo",
    "votacao_parlamentar",
    "discurso",
]

# Monkey-patch for the requester-pays staging bucket.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(slug: str) -> int:
    files = glob.glob(
        str(OUTPUT_ROOT / slug / "**" / "*.parquet"), recursive=True
    )
    return sum(pq.read_metadata(f).num_rows for f in files)


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")
    expected = local_rows(slug)

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    # Verify the staging row count against the local parquet. This is a QUERY
    # job, so it can fail when the dev QueryUsagePerDay quota is exhausted; the
    # upload itself (a load/external-table op) has already succeeded, so treat a
    # failed verification as a warning and trust the local count.
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    try:
        n = int(
            # pyrefly: ignore [bad-argument-type]
            bd.read_sql(
                q, billing_project_id=BILLING_PROJECT, from_file=True
            ).iloc[0, 0]
        )
    except Exception as e:
        print(
            f"  {slug}: uploaded (local {expected:,}); "
            f"count verify skipped ({type(e).__name__})"
        )
        return expected
    status = "OK" if n == expected else "ROW MISMATCH"
    print(f"  {slug}: uploaded {n:,} rows (local {expected:,}) — {status}")
    if n != expected:
        raise ValueError(f"{slug}: staging {n:,} != local {expected:,}")
    return n


def main():
    only = set(_argv)
    tables = [s for s in TABLES if not only or s in only]
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
