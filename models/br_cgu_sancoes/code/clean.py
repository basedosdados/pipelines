"""Cleaning code for br_cgu_sancoes (CGU sanctions registries) — bootstrap CLI.

The cleaning transform (column specs, cleaning helpers, ``process_table``,
``to_arrow``, ``build_schema``, ``build_dicionario``) lives in
``pipelines.datasets.br_cgu_sancoes.utils`` so the one-shot bootstrap and the
recurring Prefect pipeline share one implementation. This module is the
initial-load entry point: it reads the raw CGU CSV extracts already downloaded to
``input/`` and writes TYPED (date32/float64/string) partitioned Snappy Parquet
for the one-shot BigQuery upload in ``upload.py``.

The pipeline path instead writes all-STRING parquet (see
``utils.write_partitioned_string``), because ``upload_to_gcs`` infers an
all-STRING staging schema; this bootstrap keeps typed parquet, which
``bd.Table.create`` accepts directly.

Raw input  : $BR_CGU_SANCOES_INPUT  (default ~/Downloads/br_cgu_sancoes_data/input)
Parquet out: $BR_CGU_SANCOES_OUTPUT (default ~/Downloads/br_cgu_sancoes_data/output)

Output layout (hive-partitioned by snapshot date):
    output/<table>/data_extracao=YYYY-MM-DD/data.parquet
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import TypedDict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_cgu_sancoes.utils import (
    ACORDOS_COLS,
    CEIS_COLS,
    CEPIM_COLS,
    CNEP_COLS,
    DICIONARIO_ROWS,
    EFEITOS_COLS,
    build_dicionario,
    process_table,
    to_arrow,
)

# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
INPUT_DIR = Path(
    os.environ.get(
        "BR_CGU_SANCOES_INPUT",
        os.path.expanduser("~/Downloads/br_cgu_sancoes_data/input"),
    )
)
OUTPUT_DIR = Path(
    os.environ.get(
        "BR_CGU_SANCOES_OUTPUT",
        os.path.expanduser("~/Downloads/br_cgu_sancoes_data/output"),
    )
)


class TableSpec(TypedDict):
    """Specification for one raw CGU registry table (bootstrap fixed snapshot).

    Attributes:
        file: Raw CSV filename under the input directory.
        snapshot: Extraction date used as the ``data_extracao`` partition.
        cols: Ordered ``(output_column, kind)`` pairs after ``data_extracao``,
            where ``kind`` is one of ``"str"``, ``"date"``, or ``"float"``.
    """

    file: str
    snapshot: date
    cols: list[tuple[str, str]]


TABLES: dict[str, TableSpec] = {
    "ceis": {
        "file": "20260813_CEIS.csv",
        "snapshot": date(2026, 8, 13),
        "cols": CEIS_COLS,
    },
    "cnep": {
        "file": "20260813_CNEP.csv",
        "snapshot": date(2026, 8, 13),
        "cols": CNEP_COLS,
    },
    "cepim": {
        "file": "20260812_CEPIM.csv",
        "snapshot": date(2026, 8, 12),
        "cols": CEPIM_COLS,
    },
    "acordos_leniencia": {
        "file": "20260813_Acordos.csv",
        "snapshot": date(2026, 8, 13),
        "cols": ACORDOS_COLS,
    },
    "acordos_leniencia_efeitos": {
        "file": "20260813_Efeitos.csv",
        "snapshot": date(2026, 8, 13),
        "cols": EFEITOS_COLS,
    },
}


# --------------------------------------------------------------------------- #
# Typed one-shot write (kept here — the onboarding upload keeps typed parquet)
# --------------------------------------------------------------------------- #
def write_partitioned(table: pa.Table, name: str, snapshot: date) -> Path:
    """Write hive-partitioned TYPED Snappy Parquet, dropping the partition column.

    Args:
        table: Typed Arrow table including ``data_extracao``.
        name: Table slug (directory name under the output root).
        snapshot: Extraction date used as the hive partition value.

    Returns:
        Path to the written ``data.parquet`` file.
    """
    out_root = OUTPUT_DIR / name / f"data_extracao={snapshot.isoformat()}"
    out_root.mkdir(parents=True, exist_ok=True)
    file_table = table.drop(["data_extracao"])
    dest = out_root / "data.parquet"
    pq.write_table(file_table, dest, compression="snappy")
    return dest


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def validate(name: str, spec: TableSpec, dest: Path) -> None:
    """Reload the written parquet and print integrity diagnostics.

    Args:
        name: Table slug.
        spec: The table's :class:`TableSpec`.
        dest: Path to the written ``data.parquet`` file.
    """
    cols = spec["cols"]
    expected_order = ["data_extracao"] + [c[0] for c in cols]

    file_schema = pq.read_schema(dest)
    reloaded = pq.read_table(dest.parent.parent, partitioning="hive")
    df = reloaded.to_pandas()
    df = df[[c for c in expected_order if c in df.columns]]

    print(f"\n===== VALIDATE {name} =====")
    print(f"path       : {dest}")
    print(f"row count  : {len(df):,}")
    full_order = ["data_extracao", *file_schema.names]
    print(f"columns    : {full_order}")
    order_ok = full_order == expected_order
    print(f"order match: {order_ok}")
    if not order_ok:
        print(f"  EXPECTED : {expected_order}")

    print("null fraction per column:")
    for c in expected_order:
        frac = df[c].isna().mean()
        print(f"  {c}: {frac:.4f}")

    print("sample (head 3):")
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(df.head(3).to_string())


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    """Clean every registry in ``TABLES`` to typed parquet, then validate.

    Also writes the static ``dicionario`` table (all-STRING, shared with the
    pipeline). Output goes under ``OUTPUT_DIR``; nothing is uploaded here (see
    ``upload.py``).
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = {}
    for name, spec in TABLES.items():
        df = process_table(
            INPUT_DIR / spec["file"], spec["cols"], spec["snapshot"], name=name
        )
        table = to_arrow(df, spec["cols"])
        dest = write_partitioned(table, name, spec["snapshot"])
        summary[name] = (len(df), dest)

    dic_dir = build_dicionario(OUTPUT_DIR)
    summary["dicionario"] = (len(DICIONARIO_ROWS), dic_dir / "data.parquet")

    for name, spec in TABLES.items():
        validate(name, spec, summary[name][1])

    print("\n===== SUMMARY =====")
    for name, (n, dest) in summary.items():
        print(f"{name:28s} rows={n:>8,}  -> {dest}")
    print(f"\noutput root: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
