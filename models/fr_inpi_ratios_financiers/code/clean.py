"""Clean the INPI/BCE financial-ratios export into hive-partitioned staging parquet.

Usage:
    .venv/bin/python models/fr_inpi_ratios_financiers/code/clean.py

Reads ``$FR_INPI_RATIOS_DATA_DIR/input/ratios_inpi_bce.parquet`` (the ODS parquet
export of ``ratios_inpi_bce`` on data.economie.gouv.fr) and writes:

- ``output/ratios_financiers/annee=<YYYY>/data.parquet`` (one file per year)
- ``output/dicionario/data.parquet``
- ``output/_manifest.json`` with the row count per table

``FR_INPI_RATIOS_DATA_DIR`` defaults to ``~/bd_scratch/fr_inpi_ratios_financiers_data``
(not ``~/Downloads``, which is Dropbox-synced on some machines).

Staging is all-STRING: values pass through their architecture type first (so a
year serializes as ``"2021"``, not ``"2021.0"``) and are then cast to string via
arrow, never ``astype(str)`` (which would turn NULL into the literal ``"nan"``).
"""

import csv
import json
import os
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

DATA_DIR = Path(
    os.environ.get(
        "FR_INPI_RATIOS_DATA_DIR",
        Path.home() / "bd_scratch" / "fr_inpi_ratios_financiers_data",
    )
)
INPUT = DATA_DIR / "input" / "ratios_inpi_bce.parquet"
OUTPUT = DATA_DIR / "output"
ARCH_DIR = Path(__file__).resolve().parent / "architecture"

TABLE = "ratios_financiers"
PA_TYPES = {
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "STRING": pa.string(),
    "DATE": pa.date32(),
}
TYPE_BILAN = [
    ("C", "Bilan complet (régime réel normal)"),
    ("K", "Bilan consolidé"),
    ("S", "Bilan simplifié (régime simplifié, petites entreprises)"),
]


def read_arch(table: str) -> list[dict[str, str]]:
    with open(ARCH_DIR / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def build_ratios() -> int:
    arch = read_arch(TABLE)
    src = pq.read_table(INPUT)
    expected_src = {a["original_name"] for a in arch if a["original_name"]}
    missing = expected_src - set(src.column_names)
    extra = set(src.column_names) - expected_src
    if missing or extra:
        raise ValueError(
            f"Source columns changed: missing={missing} extra={extra}"
        )

    cols: dict[str, pa.Array | pa.ChunkedArray] = {}
    for a in arch:
        name, orig = a["name"], a["original_name"]
        target = PA_TYPES[a["bigquery_type"]]
        if name == "annee":
            date = src.column("date_cloture_exercice").cast(pa.date32())
            cols[name] = pc.year(date).cast(target)
        else:
            col = src.column(orig)
            if pa.types.is_string(col.type) or pa.types.is_large_string(
                col.type
            ):
                col = pc.utf8_trim_whitespace(col.cast(pa.string()))
                col = pc.if_else(pc.equal(col, ""), None, col)
            cols[name] = col.cast(target)
    typed = pa.table(cols)
    if typed.column("annee").null_count:
        raise ValueError(
            "Rows with no date_cloture_exercice: cannot partition"
        )

    string_schema = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    tdir = OUTPUT / TABLE
    shutil.rmtree(tdir, ignore_errors=True)
    years = sorted(set(typed.column("annee").to_pylist()))
    for year in years:
        part = typed.filter(pc.equal(typed.column("annee"), year))
        pdir = tdir / f"annee={year}"
        pdir.mkdir(parents=True)
        pq.write_table(
            part.cast(string_schema),
            pdir / "data.parquet",
            compression="snappy",
        )
        print(f"  annee={year}: {part.num_rows:,}", flush=True)
    return typed.num_rows


def build_dicionario() -> int:
    arch = read_arch("dicionario")
    rows = [
        {
            "id_tabela": TABLE,
            "nome_coluna": "type_bilan",
            "chave": k,
            "cobertura_temporal": None,
            "valor": v,
        }
        for k, v in TYPE_BILAN
    ]
    schema = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    tdir = OUTPUT / "dicionario"
    shutil.rmtree(tdir, ignore_errors=True)
    tdir.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema),
        tdir / "data.parquet",
        compression="snappy",
    )
    return len(rows)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    manifest = {TABLE: build_ratios(), "dicionario": build_dicionario()}
    (OUTPUT / "_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest), flush=True)


if __name__ == "__main__":
    main()
