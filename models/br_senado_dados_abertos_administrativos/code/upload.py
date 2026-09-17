"""Upload cleaned br_senado_dados_abertos_administrativos parquet to BigQuery.

Usage:
    uv run python models/br_senado_dados_abertos_administrativos/code/upload.py \
        [--env dev|prod] [table ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. This
machine's config.toml is dev-only; prod table data is materialised by the
table-approve action when the onboarding PR merges, never uploaded from here.

Uploads smallest first, verifies the staging row count against the local
parquet, and stops on the first failure.
"""

import glob
import os
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
DATASET_ID = "br_senado_dados_abertos_administrativos"
DATA_DIR = os.environ.get(
    "SENADO_ADM_DATA", os.path.expanduser(f"~/Downloads/{DATASET_ID}_data")
)
OUTPUT_ROOT = Path(DATA_DIR) / "output"

# Smallest first, so a credentials or schema problem surfaces on a cheap table
# rather than after uploading millions of remuneração rows.
TABLES = [
    "senador",
    "senador_gabinete",
    "senador_escritorio_apoio",
    "senador_auxilio_moradia",
    "senador_aposentado_pensionista",
    "menor_aprendiz",
    "quadro_pessoal",
    "diretor_coordenador",
    "servidor_cedido",
    "estagiario",
    "dicionario",
    "suprido_ato_concessao",
    "suprido_empenho",
    "ata_acionamento",
    "contrato_aditivo",
    "contratacao_garantia",
    "previsao_aposentadoria",
    "pensionista",
    "empresa",
    "licitacao",
    "terceirizado",
    "servidor_aposentado",
    "suprido_movimentacao",
    "servidor_exonerado",
    "suprido_transacao",
    "suprido_movimentacao_subtipo",
    "suprido_transacao_objeto",
    "servidor_ativo",
    "licitacao_detalhamento",
    "contratacao_orgao_gestor",
    "contratacao_pagamento",
    "contratacao_pagamento_empenho",
    "contratacao",
    "contratacao_documento_fiscal",
    "contratacao_item",
    "servidor",
    "servidor_hora_extra",
    "servidor_hora_extra_dia",
    "despesa_ceaps",
    "servidor_remuneracao",
]

# The staging bucket is requester-pays.
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
    if expected == 0:
        # A table can be legitimately empty for the extracted window — the
        # supridos movimentações are, in some years. Nothing to upload.
        print(f"  {slug}: no rows in this extraction, skipped")
        return 0

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as exc:
        print(f"  [warn] staging prefix cleanup: {exc}")

    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    # Verification is a QUERY job, so it can fail when the dev QueryUsagePerDay
    # quota is exhausted. The upload itself is a load/external-table operation
    # and has already succeeded, so a failed verification is a warning.
    query = (
        f"select count(*) as n "
        f"from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    )
    try:
        got = int(
            bd.read_sql(
                query, billing_project_id=BILLING_PROJECT, from_file=True
            ).iloc[0, 0]
        )
    except Exception as exc:
        print(
            f"  {slug}: uploaded (local {expected:,}); "
            f"count verify skipped ({type(exc).__name__})"
        )
        return expected
    status = "OK" if got == expected else "ROW MISMATCH"
    print(f"  {slug}: uploaded {got:,} rows (local {expected:,}) — {status}")
    if got != expected:
        raise ValueError(f"{slug}: staging {got:,} != local {expected:,}")
    return got


def main() -> None:
    only = set(_argv)
    unknown = only - set(TABLES)
    if unknown:
        sys.exit(f"unknown table(s): {', '.join(sorted(unknown))}")
    tables = [s for s in TABLES if not only or s in only]
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
    print(f"    from {OUTPUT_ROOT}", flush=True)
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as exc:
            print(f"  FAILED: {type(exc).__name__}: {exc}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
