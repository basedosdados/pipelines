"""Pure transform for br_pncp: raw PNCP JSON -> partitioned staging parquet.

No Prefect imports here. The one-shot onboarding (``clean.py``) and the
recurring pipeline both import these functions, so the cleaning logic exists in
exactly one place.

Two house conventions shape the output:

* Staging parquet is **all-STRING**. The dbt model ``safe_cast``s every column
  to its architecture type, and ``upload_to_gcs`` infers the staging schema from
  a stringified one-row header, so typed parquet is rejected downstream. The
  cast goes through arrow rather than ``astype(str)``, which would render NULL
  as the literal ``"nan"`` — a value ``safe_cast`` cannot turn back into NULL.
* Real types are applied *before* the string cast, so an INT64 year serializes
  as ``"2025"`` rather than ``"2025.0"``.
"""

from __future__ import annotations

import csv
import gzip
import json
import shutil
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ARCHITECTURE_DIR = Path(__file__).parent / "architecture"

# Grain: the column(s) that uniquely identify a row, and the column used to pick
# the surviving version when the same key appears in several harvest windows.
DEDUP_KEYS = {
    "contratacao": (["id_contratacao_pncp"], "data_atualizacao"),
    "contrato": (["id_contrato_pncp"], "data_atualizacao"),
    "ata_registro_preco": (["id_ata_pncp"], "data_atualizacao"),
    "instrumento_cobranca": (
        [
            "cnpj_orgao",
            "ano_contrato",
            "sequencial_contrato",
            "sequencial_instrumento_cobranca",
        ],
        "data_atualizacao",
    ),
    "plano_contratacao_anual": (
        ["id_pca_pncp", "numero_item"],
        "data_atualizacao",
    ),
}

# Which raw field the partition year is derived from, per table.
PARTITION_SOURCE = {
    "contratacao": "dataPublicacaoPncp",
    "contrato": "dataPublicacaoPncp",
    "ata_registro_preco": "dataPublicacaoPncp",
    "instrumento_cobranca": "dataInclusao",
    "plano_contratacao_anual": "anoPca",
}

# Tables whose API payload nests a list that must be exploded into one row per
# element, mapping table -> the raw array field.
EXPLODE = {"plano_contratacao_anual": "itens"}


def read_architecture(table: str) -> list[dict]:
    with (ARCHITECTURE_DIR / f"{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def dig(record: dict, path: str):
    """Resolve a dotted original_name against a nested payload."""
    cur = record
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def as_date(value):
    """Normalise PNCP date and date-time strings to an ISO date."""
    if value in (None, ""):
        return None
    text = str(value)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[: len(text)], fmt).date().isoformat()
        except ValueError:
            continue
    # Fall back to the leading yyyy-mm-dd when the tail is unexpected.
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def as_number(value, integer: bool):
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return str(int(number)) if integer else repr(number)


def as_bool(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value).strip().lower()
    return {"true": "true", "false": "false"}.get(text)


def as_string(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value).strip()
    return text or None


def convert(value, bq_type: str):
    """Cast one raw value to its final *string* representation.

    The real type is applied first so the string form is the one the dbt
    ``safe_cast`` expects; the result is always ``str | None``.
    """
    if bq_type == "DATE":
        return as_date(value)
    if bq_type == "INT64":
        return as_number(value, integer=True)
    if bq_type == "FLOAT64":
        return as_number(value, integer=False)
    if bq_type == "BOOLEAN":
        return as_bool(value)
    return as_string(value)


def partition_year(record: dict, table: str) -> int | None:
    raw = record.get(PARTITION_SOURCE[table])
    if raw in (None, ""):
        return None
    if table == "plano_contratacao_anual":
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    iso = as_date(raw)
    return int(iso[:4]) if iso else None


def flatten(record: dict, table: str, columns: list[dict]) -> list[dict]:
    """Turn one API record into one or more flat, all-string rows."""
    array_field = EXPLODE.get(table)
    children = record.get(array_field) or [{}] if array_field else [{}]

    rows = []
    for child in children:
        row = {}
        for spec in columns:
            name, bq_type, original = (
                spec["name"],
                spec["bigquery_type"],
                spec["original_name"],
            )
            if name == "ano":
                row[name] = None  # filled from the partition below
                continue
            if not original:
                row[name] = None
                continue
            if array_field and original.startswith(f"{array_field}."):
                raw = dig(child, original.split(".", 1)[1])
            else:
                raw = dig(record, original)
            row[name] = convert(raw, bq_type)
        year = partition_year(record, table)
        row["ano"] = str(year) if year is not None else None
        rows.append(row)
    return rows


def iter_raw(input_dir: Path, table: str):
    for path in sorted((input_dir / table).glob("*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def clean_table(input_dir: Path, output_dir: Path, table: str) -> dict:
    """Read every raw chunk for one table, dedupe, and write partitioned parquet.

    Returns a summary with the raw and written row counts so the caller can
    assert the transform reproduces a known result.
    """
    columns = read_architecture(table)
    names = [c["name"] for c in columns]
    key_cols, recency_col = DEDUP_KEYS[table]

    # key -> row, keeping the most recently updated version. PNCP re-publishes a
    # record into every window it was touched in, so the same id legitimately
    # appears many times across chunks.
    best: dict[tuple, dict] = {}
    raw_rows = 0
    undated = 0

    for record in iter_raw(input_dir, table):
        for row in flatten(record, table, columns):
            raw_rows += 1
            if row["ano"] is None:
                undated += 1
                continue
            key = tuple(row.get(k) for k in key_cols)
            incumbent = best.get(key)
            if incumbent is None or (row.get(recency_col) or "") >= (
                incumbent.get(recency_col) or ""
            ):
                best[key] = row

    by_year: dict[str, list[dict]] = {}
    for row in best.values():
        by_year.setdefault(row["ano"], []).append(row)

    # The partition column lives in the directory name only. Writing it into the
    # file as well makes the hive-partitioned dataset unreadable: the path-derived
    # key and the in-file column collide ("Field ano has incompatible types:
    # string vs dictionary<values=int32>").
    file_names = [n for n in names if n != "ano"]
    schema = pa.schema([(n, pa.string()) for n in file_names])

    # Rewrite the table's tree from scratch so a partition that disappears
    # between runs does not survive as a stale file in the staging upload.
    table_dir = output_dir / table
    if table_dir.exists():
        shutil.rmtree(table_dir)

    written = 0
    for year, rows in sorted(by_year.items()):
        target = output_dir / table / f"ano={year}"
        target.mkdir(parents=True, exist_ok=True)
        arrays = [
            pa.array([r.get(n) for r in rows], type=pa.string())
            for n in file_names
        ]
        pq.write_table(
            pa.Table.from_arrays(arrays, schema=schema),
            target / "data.parquet",
            compression="snappy",
        )
        written += len(rows)

    return {
        "table": table,
        "raw_rows": raw_rows,
        "deduped_rows": written,
        "undated_dropped": undated,
        "years": sorted(by_year),
    }
