"""Clean Censo 2022 public CSVs into all-STRING hive-partitioned parquet.

Streams each UF CSV in chunks so SP/MG never sit fully in memory.

Usage:
    uv run python models/br_ibge_censo_demografico/code/clean.py [--ufs RR] [--delete-zip]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
import constants as c

HIVE_ONLY = {"ano", "sigla_uf"}
CHUNKSIZE = 50_000


def architecture_rows(slug: str) -> list[dict[str, str]]:
    path = c.ARCHITECTURE_DIR / f"{slug}.csv"
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def parquet_columns(rows: list[dict[str, str]]) -> list[str]:
    return [row["name"] for row in rows if row["name"] not in HIVE_ONLY]


def to_string_table(frame: pd.DataFrame, columns: list[str]) -> pa.Table:
    ordered = frame.reindex(columns=columns)
    arrays = []
    for name in columns:
        values = ordered[name].to_numpy(dtype=object)
        values[pd.isna(values)] = None
        arrays.append(pa.array(values, type=pa.string()))
    return pa.table(dict(zip(columns, arrays, strict=True)))


def _wanted(slug: str, only: set[str]) -> bool:
    if not only:
        return True
    token = slug.replace("microdados_", "").replace("_2022", "")
    return slug in only or token in only


def clean_uf(sigla: str, zip_path: Path, only: set[str]) -> dict[str, int]:
    code = next(cd for cd, sg, _ in c.UF_ZIPS if sg == sigla)
    counts: dict[str, int] = {}
    with zipfile.ZipFile(zip_path) as zf:
        names = {Path(n).name: n for n in zf.namelist()}
        for sheet, spec in c.TABLES.items():
            slug = spec["slug"]
            if not _wanted(slug, only):
                continue
            csv_name = f"{spec['csv_prefix']}_{code}_publico.csv"
            member = names.get(csv_name)
            if member is None:
                raise FileNotFoundError(f"{csv_name} not in {zip_path.name}")
            columns = parquet_columns(architecture_rows(slug))
            dest_dir = (
                c.OUTPUT_DIR / slug / f"ano={c.YEAR}" / f"sigla_uf={sigla}"
            )
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / "data.parquet"
            total = 0
            writer: pq.ParquetWriter | None = None
            # Materialise the member to a seekable temp file. Chunked
            # pandas reads from a zip stream can split mid-row.
            with (
                zf.open(member) as raw,
                tempfile.NamedTemporaryFile(
                    suffix=".csv", delete=False
                ) as tmp,
            ):
                while True:
                    block = raw.read(8 * 1024 * 1024)
                    if not block:
                        break
                    tmp.write(block)
                tmp_path = Path(tmp.name)
            try:
                chunks = pd.read_csv(
                    tmp_path,
                    sep=";",
                    dtype=str,
                    keep_default_na=False,
                    na_values=[""],
                    encoding="latin-1",
                    encoding_errors="replace",
                    on_bad_lines="warn",
                    chunksize=CHUNKSIZE,
                )
                for frame in chunks:
                    frame.columns = [
                        c.RENAMES[sheet].get(col.strip(), col.strip().lower())
                        for col in frame.columns
                    ]
                    if "sigla_uf" in frame.columns:
                        frame["sigla_uf"] = (
                            frame["sigla_uf"]
                            .astype(str)
                            .str.strip()
                            .map(
                                lambda v: c.UF_CODE_TO_SIGLA.get(v.zfill(2), v)
                            )
                        )
                    table = to_string_table(frame, columns)
                    if total == 0:
                        probe = next(
                            (
                                name
                                for name in (
                                    "d0130",
                                    "p0150",
                                    "f0150",
                                    "m0150",
                                )
                                if name in columns
                            ),
                            columns[-1],
                        )
                        filled = (
                            table.column(probe).null_count < table.num_rows
                        )
                        print(
                            f"    probe {probe} non-null={table.num_rows - table.column(probe).null_count}/{table.num_rows}",
                            flush=True,
                        )
                        if not filled:
                            raise ValueError(
                                f"{slug} {sigla}: {probe} is all-null in the first chunk"
                            )
                    if writer is None:
                        writer = pq.ParquetWriter(
                            dest, table.schema, compression="snappy"
                        )
                    writer.write_table(table)
                    total += table.num_rows
            finally:
                tmp_path.unlink(missing_ok=True)
            if writer is not None:
                writer.close()
            counts[slug] = total
            print(f"  {slug} {sigla}: {total:,} rows → {dest}", flush=True)
    return counts


def write_dicionario() -> int:
    src = c.ARCHITECTURE_DIR / "dicionario.csv"
    frame = pd.read_csv(src, dtype=str, keep_default_na=False)
    columns = [
        "id_tabela",
        "nome_coluna",
        "chave",
        "cobertura_temporal",
        "valor",
    ]
    table = to_string_table(frame, columns)
    dest_dir = c.OUTPUT_DIR / "dicionario"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "data.parquet"
    pq.write_table(table, dest, compression="snappy")
    print(f"  dicionario: {table.num_rows:,} rows → {dest}", flush=True)
    return table.num_rows


def _save_counts(counts: dict) -> None:
    path = c.DATA_ROOT / "row_counts.json"
    existing: dict = {}
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
    existing.update(counts)
    path.write_text(
        json.dumps(existing, indent=2, sort_keys=True), encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ufs", default="", help="Comma-separated siglas")
    parser.add_argument(
        "--tables",
        default="",
        help="pessoa,domicilio,familia,mortalidade and/or slugs",
    )
    parser.add_argument(
        "--delete-zip",
        action="store_true",
        help="Delete the UF zip after a successful clean",
    )
    args = parser.parse_args()
    wanted = {u.strip().upper() for u in args.ufs.split(",") if u.strip()}
    only = {t.strip().lower() for t in args.tables.split(",") if t.strip()}
    all_counts: dict[str, dict[str, int]] = {}
    for _code, sigla, zip_name in c.UF_ZIPS:
        if wanted and sigla not in wanted:
            continue
        zip_path = c.INPUT_DIR / zip_name
        if not zip_path.exists():
            raise FileNotFoundError(f"missing {zip_path}; run download.py")
        print(f"=== {sigla} ===", flush=True)
        all_counts[sigla] = clean_uf(sigla, zip_path, only)
        if args.delete_zip:
            zip_path.unlink()
            print(f"  deleted {zip_name}", flush=True)
    if not only or "dicionario" in only:
        all_counts["dicionario"] = {"dicionario": write_dicionario()}
    _save_counts(all_counts)


if __name__ == "__main__":
    main()
