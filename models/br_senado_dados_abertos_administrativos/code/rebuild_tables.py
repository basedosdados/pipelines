"""Re-extract selected tables into the existing output tree.

  uv run python models/br_senado_dados_abertos_administrativos/code/rebuild_tables.py <table> [...]

Used when a transform fix changes only some tables and re-running the whole
extraction (hours, most of it the contratação fan-out) would be wasteful. Writes
through the same shared transform and the same writer as run_onboarding, so the
output is identical to what a full run would produce.
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import os

import pyarrow.parquet as pq

from pipelines.datasets.br_senado_dados_abertos_administrativos import (
    senado_adm_api as api,
)
from pipelines.datasets.br_senado_dados_abertos_administrativos import (
    senado_adm_clean as clean,
)
from pipelines.datasets.br_senado_dados_abertos_administrativos.utils import (
    FIRST_YEAR_HORA_EXTRA,
    FIRST_YEAR_REMUNERACAO,
    TABLES,
    months,
    write_partitioned,
)

DATASET = "br_senado_dados_abertos_administrativos"
DATA_DIR = os.environ.get(
    "SENADO_ADM_DATA", os.path.expanduser(f"~/Downloads/{DATASET}_data")
)
OUTPUT = os.path.join(DATA_DIR, "output")


def rows_in(table: str) -> int:
    files = glob.glob(
        os.path.join(OUTPUT, table, "**", "*.parquet"), recursive=True
    )
    return sum(pq.read_metadata(f).num_rows for f in files)


def emit(table: str, rows: list[dict], reset: bool = True) -> None:
    write_partitioned(
        rows, OUTPUT, table, TABLES[table]["partition"], reset=reset
    )


def rebuild_servidor_remuneracao(today: dt.date) -> None:
    for i, year in enumerate(range(FIRST_YEAR_REMUNERACAO, today.year + 1)):
        rows = clean.build_servidor_remuneracao(months([year], today))
        emit("servidor_remuneracao", rows, reset=i == 0)
        print(f"    {year}: {len(rows):,} rows", flush=True)


def rebuild_servidor_hora_extra(today: dt.date) -> None:
    for i, year in enumerate(range(FIRST_YEAR_HORA_EXTRA, today.year + 1)):
        parents, days = clean.build_servidor_hora_extra(months([year], today))
        emit("servidor_hora_extra", parents, reset=i == 0)
        emit("servidor_hora_extra_dia", days, reset=i == 0)
        print(
            f"    {year}: {len(parents):,} parents, {len(days):,} days",
            flush=True,
        )


def rebuild_contratacao_orgao_gestor(today: dt.date) -> None:
    raw = api.fetch_contratacoes()
    rows = clean.build_contratacao_orgao_gestor(raw, today.isoformat())
    emit("contratacao_orgao_gestor", rows)
    print(f"    {len(rows):,} rows", flush=True)


REBUILDERS = {
    "servidor_remuneracao": rebuild_servidor_remuneracao,
    "servidor_hora_extra": rebuild_servidor_hora_extra,
    "contratacao_orgao_gestor": rebuild_contratacao_orgao_gestor,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("tables", nargs="+", choices=sorted(REBUILDERS))
    args = ap.parse_args()
    today = dt.date.today()
    for table in args.tables:
        print(f"=== {table} ===", flush=True)
        REBUILDERS[table](today)
    print("\n=== RESULT ===")
    for table in (
        "servidor_remuneracao",
        "servidor_hora_extra",
        "servidor_hora_extra_dia",
        "contratacao_orgao_gestor",
    ):
        if os.path.isdir(os.path.join(OUTPUT, table)):
            print(f"  {table:30} {rows_in(table):>10,} rows")


if __name__ == "__main__":
    main()
