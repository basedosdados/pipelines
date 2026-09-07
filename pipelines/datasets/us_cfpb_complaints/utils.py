"""Pure download and transform helpers for us_cfpb_complaints — no Prefect imports.

This module is the single home of the cleaning transform. The one-shot onboarding
scripts under ``models/us_cfpb_complaints/code/`` import these functions rather than
duplicating them, so the recurring pipeline and the bootstrap can never drift.

Two decisions are load-bearing and documented where they are made:

* The CSV is read with Python's ``csv`` module. Narratives contain newlines inside
  quoted fields and DuckDB's parallel reader desynchronises on them; the file itself
  is well-formed RFC4180.
* Output parquet is **all-STRING**. ``pipelines.utils.gcs.dump_header`` stringifies
  the header BigQuery infers the staging schema from, so typed parquet is rejected.
  The dbt model ``safe_cast``s every column to its architecture type.
"""

import csv
import shutil
import subprocess
import sys
import time
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_cfpb_complaints.constants import constants

csv.field_size_limit(sys.maxsize)

DOWNLOAD_CHUNK = 8 * 1024 * 1024


@dataclass
class Col:
    """One column of an architecture table.

    The transform only needs ``name``/``bq_type``/``original``; the remaining fields
    are carried so the onboarding metadata scripts can read the same TSV through the
    same parser instead of writing a second one.
    """

    name: str  # clean BigQuery column name
    bq_type: str  # INT64 / STRING / DATE
    original: str  # header token in the source CSV, empty if derived
    covered_by_dictionary: bool = False
    directory_column: str = ""
    measurement_unit: str = ""


def load_cols(table: str) -> list[Col]:
    """Load ordered column specs from the architecture TSV for a table."""
    path = Path(constants.ARCHITECTURE_DIR.value) / f"sheet_{table}.tsv"
    cols = []
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            cols.append(
                Col(
                    name=r["name"].strip(),
                    bq_type=r["bigquery_type"].strip().upper(),
                    original=r["original_name"].strip(),
                    covered_by_dictionary=(
                        r["covered_by_dictionary"].strip() == "yes"
                    ),
                    directory_column=r["directory_column"].strip(),
                    measurement_unit=r["measurement_unit"].strip(),
                )
            )
    return cols


def state_id_for(published_state: str) -> str | None:
    """Map a published ``State`` value to its FIPS code, or None when there is none.

    Returns None for the empty value, for the military post codes AA/AE/AP, and for
    anything else absent from the directory — never a guess.
    """
    v = (published_state or "").strip()
    if not v:
        return None
    v = constants.STATE_ALIASES.value.get(v.upper(), v)
    return constants.STATE_FIPS.value.get(v.upper())


def source_last_modified() -> str:
    """Return the export's ``Last-Modified`` header, without downloading it."""
    r = requests.head(
        constants.BULK_URL.value, timeout=60, allow_redirects=True
    )
    r.raise_for_status()
    return r.headers.get("last-modified", "")


def download_snapshot(input_dir: Path) -> Path:
    """Download and unzip the full-database export into ``input_dir``.

    Returns:
        Path to the unzipped ``complaints.csv``.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    zip_path = input_dir / constants.ZIP_NAME.value
    csv_path = input_dir / constants.CSV_NAME.value

    r = requests.head(
        constants.BULK_URL.value, timeout=60, allow_redirects=True
    )
    r.raise_for_status()
    size = int(r.headers.get("content-length", 0))
    print(f"source: {constants.BULK_URL.value}")
    print(f"  last-modified: {r.headers.get('last-modified', '')}")
    print(f"  size: {size / 1e9:.2f} GB")

    t0 = time.time()
    got = 0
    with requests.get(
        constants.BULK_URL.value, stream=True, timeout=(30, 300)
    ) as resp:
        resp.raise_for_status()
        # decode_content=True so a Content-Encoding'd body is decompressed rather
        # than written as raw transport bytes.
        resp.raw.decode_content = True
        with open(zip_path, "wb") as fh:
            while True:
                block = resp.raw.read(DOWNLOAD_CHUNK)
                if not block:
                    break
                fh.write(block)
                got += len(block)
    if size and zip_path.stat().st_size != size:
        raise RuntimeError(
            f"short download: got {zip_path.stat().st_size} bytes, expected {size}"
        )
    print(f"  downloaded {got / 1e9:.2f} GB in {time.time() - t0:.0f}s")

    if shutil.which("unzip"):
        subprocess.run(
            ["unzip", "-o", str(zip_path), "-d", str(input_dir)],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    else:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(input_dir)
    if not csv_path.exists():
        raise RuntimeError(f"expected {csv_path} after unzip")
    # Free the ~1.4 GB archive: the worker pod's disk is not generous.
    zip_path.unlink()
    print(f"  unzipped: {csv_path.stat().st_size / 1e9:.2f} GB")
    return csv_path


def clean_complaint(
    csv_path: Path, output_dir: Path, limit: int | None = None
) -> dict:
    """Stream the bulk CSV into ``<output_dir>/complaint/year=<YYYY>/data.parquet``.

    Args:
        csv_path: The unzipped ``complaints.csv``.
        output_dir: Root output directory.
        limit: Stop after this many source records (for smoke tests).

    Returns:
        ``{"per_year": {...}, "stats": {...}, "max_date_received": "YYYY-MM-DD"}``.
    """
    table_slug = constants.COMPLAINT.value
    cols = load_cols(table_slug)
    order = [c.name for c in cols]
    schema = pa.schema([pa.field(n, pa.string()) for n in order])
    src_of = {c.name: c.original for c in cols}

    tdir = output_dir / table_slug
    tdir.mkdir(parents=True, exist_ok=True)

    writers: dict[str, pq.ParquetWriter] = {}
    buf: dict[str, list[list]] = defaultdict(list)
    per_year: Counter = Counter()
    stats: Counter = Counter()
    max_date = ""
    flush_rows = constants.FLUSH_ROWS.value
    t0 = time.time()

    def flush(year: str) -> None:
        rows = buf[year]
        if not rows:
            return
        arrays = [
            pa.array([r[i] for r in rows], type=pa.string())
            for i in range(len(order))
        ]
        w = writers.get(year)
        if w is None:
            pdir = tdir / f"year={year}"
            pdir.mkdir(parents=True, exist_ok=True)
            w = pq.ParquetWriter(
                pdir / "data.parquet", schema, compression="snappy"
            )
            writers[year] = w
        w.write_table(pa.Table.from_arrays(arrays, schema=schema))
        buf[year] = []

    n = 0
    with open(csv_path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = [
            c.original
            for c in cols
            if c.original and c.original not in (reader.fieldnames or [])
        ]
        if missing:
            raise RuntimeError(
                f"source CSV is missing expected columns: {missing}\n"
                f"header seen: {reader.fieldnames}"
            )
        extra = [
            f
            for f in (reader.fieldnames or [])
            if f not in set(src_of.values())
        ]
        if extra:
            # Not fatal, but never silent: a new source column is a decision.
            print(f"WARNING: source columns not in the architecture: {extra}")

        for rec in reader:
            n += 1
            date_received = (rec["Date received"] or "").strip()
            year = date_received[:4]
            if len(date_received) != 10 or not year.isdigit():
                stats["bad_date_received"] += 1
                continue
            if date_received > max_date:
                max_date = date_received

            published_state = (rec["State"] or "").strip()
            sid = state_id_for(published_state)
            if published_state and sid is None:
                stats["state_without_fips"] += 1

            out = []
            for c in cols:
                if c.name == "year":
                    out.append(year)
                elif c.name == "state_id":
                    out.append(sid)
                else:
                    v = rec[c.original]
                    v = v.strip() if v is not None else ""
                    out.append(v if v != "" else None)
            buf[year].append(out)
            per_year[year] += 1
            if len(buf[year]) >= flush_rows:
                flush(year)
            if n % 2_000_000 == 0:
                print(f"  ...{n:,} rows ({time.time() - t0:.0f}s)", flush=True)
            if limit and n >= limit:
                break

    for year in list(buf):
        flush(year)
    for w in writers.values():
        w.close()

    stats["source_rows"] = n
    stats["written_rows"] = sum(per_year.values())
    print(
        f"complaint: {stats['written_rows']:,} rows across {len(per_year)} year "
        f"partitions in {time.time() - t0:.0f}s"
    )
    return {
        "per_year": dict(per_year),
        "stats": dict(stats),
        "max_date_received": max_date,
    }


def build_dicionario(output_dir: Path) -> int:
    """Build ``<output_dir>/dicionario/data.parquet`` from the cleaned complaint parquet.

    Regenerated on every run: a taxonomy revision introduces new values, and the
    complaint table's ``custom_dictionary_coverage`` test fails if the register
    lags behind the data.

    Returns:
        The number of dictionary rows written.
    """
    complaint_dir = output_dir / constants.COMPLAINT.value
    parts = sorted(complaint_dir.glob("year=*/data.parquet"))
    if not parts:
        raise RuntimeError(f"no complaint parquet under {complaint_dir}")

    dict_columns = constants.DICT_COLUMNS.value
    span: dict[tuple[str, str], list[int]] = defaultdict(lambda: [9999, 0])
    for p in parts:
        year = int(p.parent.name.split("=", 1)[1])
        # ParquetFile.read, not pq.read_table: the latter infers a hive dataset
        # from the `year=<YYYY>` directory and then refuses to merge that inferred
        # `year` with the STRING `year` inside the file.
        tbl = pq.ParquetFile(p).read(columns=dict_columns)
        for col in dict_columns:
            for v in tbl.column(col).unique().to_pylist():
                if v is None or v == "":
                    continue
                s = span[(col, v)]
                s[0] = min(s[0], year)
                s[1] = max(s[1], year)

    rows = [
        {
            "id_tabela": constants.COMPLAINT.value,
            "nome_coluna": col,
            "chave": value,
            # Data Basis temporal-coverage notation: START(INTERVAL)END
            "cobertura_temporal": f"{y0}(1){y1}",
            "valor": value,
        }
        for (col, value), (y0, y1) in sorted(span.items())
    ]

    order = [c.name for c in load_cols(constants.DICIONARIO.value)]
    schema = pa.schema([pa.field(n, pa.string()) for n in order])
    table = pa.Table.from_arrays(
        [pa.array([r[n] for r in rows], type=pa.string()) for n in order],
        schema=schema,
    )
    ddir = output_dir / constants.DICIONARIO.value
    ddir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, ddir / "data.parquet", compression="snappy")
    print(f"dicionario: {len(rows)} rows")
    return len(rows)


def assert_all_string(output_dir: Path) -> None:
    """Fail if any output parquet column is not STRING, or the order drifted.

    A typed staging column matches the dev table built from the same parquet and
    only fails once the flow recreates the table from a stringified header — i.e.
    on the first prod run, which is the worst place to find out.
    """
    for table in constants.ALL_TABLES.value:
        expected = [c.name for c in load_cols(table)]
        for p in sorted((output_dir / table).rglob("*.parquet")):
            sch = pq.ParquetFile(p).schema_arrow
            if list(sch.names) != expected:
                raise RuntimeError(
                    f"{p}: column order/name mismatch {list(sch.names)}"
                )
            bad = [
                (nm, str(t))
                for nm, t in zip(sch.names, sch.types, strict=True)
                if str(t) != "string"
            ]
            if bad:
                raise RuntimeError(f"{p}: non-string columns {bad}")


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Clean the snapshot into both tables and verify the staging schema.

    Returns:
        Table slug -> output directory, plus ``"max_date_received"`` (the poll's
        source date) and ``"row_counts"``.
    """
    csv_path = input_dir / constants.CSV_NAME.value
    res = clean_complaint(csv_path, output_dir)
    n_dic = build_dicionario(output_dir)
    assert_all_string(output_dir)
    return {
        constants.COMPLAINT.value: output_dir / constants.COMPLAINT.value,
        constants.DICIONARIO.value: output_dir / constants.DICIONARIO.value,
        "max_date_received": res["max_date_received"],
        "row_counts": {
            constants.COMPLAINT.value: res["stats"]["written_rows"],
            constants.DICIONARIO.value: n_dic,
        },
        "per_year": res["per_year"],
    }
