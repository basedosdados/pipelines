"""Clean JMED (Japanese Municipal Elections Dataset) into all-STRING staging parquet.

Usage:
    python models/jp_yale_jmed/code/clean_data.py

Reads ``$JP_YALE_JMED_DATA_DIR/input/{jmed_v1.0,municipal_history}.csv``
(Harvard Dataverse, https://doi.org/10.7910/DVN/E9ZUNV, files 14367635 and
14367637 with ``?format=original``) and writes

* ``output/candidate/year=YYYY/data.parquet``
* ``output/municipal_history/data.parquet``
* ``output/dicionario/data.parquet``

``JP_YALE_JMED_DATA_DIR`` defaults to ``~/Library/Caches/jp_yale_jmed_data``
(kept out of the repo and out of Dropbox).

The transform mirrors the published files. It renames ``election_year`` to
``year``, zero-pads ``prefecture_code`` to two characters, orders columns as in
the architecture, and turns the source's ``NA`` into NULL. Coded columns are
checked against the dictionary and the script fails on any unknown value.

The output directory is deleted before it is rewritten, so a re-run never keeps
parquet produced by an older version of this code. Row counts go to
``output/_manifest_<table>.json``.
"""

import csv
import json
import os
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = Path(
    os.environ.get(
        "JP_YALE_JMED_DATA_DIR",
        Path.home() / "Library" / "Caches" / "jp_yale_jmed_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
ARCH_DIR = Path(__file__).parent / "architecture"

RENAME = {"election_year": "year"}
EXPECTED_ROWS = {"candidate": 223_561, "municipal_history": 2_053}

# column -> {key: Portuguese label}; written to the dicionario table.
DICTIONARY = {
    "unified": {"0": "Não", "1": "Sim"},
    "uncontested": {"0": "Não", "1": "Sim"},
    "incumbent": {"0": "Não", "1": "Sim"},
    "elected": {"0": "Não", "1": "Sim"},
    "sex": {"M": "Masculino", "F": "Feminino"},
}


def read_columns(table: str) -> list[str]:
    with open(ARCH_DIR / f"{table}.csv", newline="", encoding="utf-8") as f:
        return [row["name"] for row in csv.DictReader(f)]


def read_source(name: str) -> list[dict[str, str | None]]:
    """Read a source CSV as strings, mapping NA and empty cells to None."""
    with open(INPUT / name, newline="", encoding="utf-8-sig") as f:
        return [
            {
                RENAME.get(k, k): (None if v in ("NA", "") else v)
                for k, v in row.items()
            }
            for row in csv.DictReader(f)
        ]


def to_table(rows: list[dict], cols: list[str]) -> pa.Table:
    missing = set(cols) - set(rows[0])
    extra = set(rows[0]) - set(cols)
    if missing or extra:
        raise ValueError(f"Columns differ: missing {missing}, extra {extra}")
    schema = pa.schema([pa.field(c, pa.string()) for c in cols])
    return pa.Table.from_pydict(
        {c: [r[c] for r in rows] for c in cols}, schema=schema
    )


def write_manifest(table: str, rows: int, **extra) -> None:
    if rows != EXPECTED_ROWS.get(table, rows):
        raise ValueError(
            f"{table}: {rows:,} rows, codebook says {EXPECTED_ROWS[table]:,}"
        )
    (OUTPUT / f"_manifest_{table}.json").write_text(
        json.dumps({"table": table, "rows": rows, **extra}, indent=2)
    )
    print(f"  {table}: {rows:,} rows", flush=True)


def clean_candidate() -> None:
    rows = read_source("jmed_v1.0.csv")
    by_year: dict[str, list[dict]] = {}
    for r in rows:
        year, pref = r["year"], r["prefecture_code"]
        if year is None or pref is None:
            raise ValueError(
                f"{r['candidate_id']}: missing year or prefecture"
            )
        r["prefecture_code"] = pref.zfill(2)
        for col, labels in DICTIONARY.items():
            if r[col] is not None and r[col] not in labels:
                raise ValueError(f"{r['candidate_id']}: {col}={r[col]!r}")
        by_year.setdefault(year, []).append(r)
    cols = read_columns("candidate")
    shutil.rmtree(OUTPUT / "candidate", ignore_errors=True)
    for year, part in sorted(by_year.items()):
        pdir = OUTPUT / "candidate" / f"year={year}"
        pdir.mkdir(parents=True)
        # year lives in the hive path; BigQuery rebuilds it from there.
        table = to_table(part, cols).drop_columns(["year"])
        pq.write_table(table, pdir / "data.parquet", compression="snappy")
    write_manifest(
        "candidate",
        len(rows),
        rows_by_year={y: len(p) for y, p in sorted(by_year.items())},
    )


def clean_municipal_history() -> None:
    rows = read_source("municipal_history.csv")
    out = OUTPUT / "municipal_history"
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    table = to_table(rows, read_columns("municipal_history"))
    pq.write_table(table, out / "data.parquet", compression="snappy")
    write_manifest("municipal_history", len(rows))


def build_dicionario() -> None:
    rows = [
        {
            "id_tabela": "candidate",
            "nome_coluna": col,
            "chave": key,
            "cobertura_temporal": None,
            "valor": label,
        }
        for col, labels in DICTIONARY.items()
        for key, label in labels.items()
    ]
    out = OUTPUT / "dicionario"
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    table = to_table(rows, read_columns("dicionario"))
    pq.write_table(table, out / "data.parquet", compression="snappy")
    write_manifest("dicionario", len(rows))


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    print(f"=== cleaning {INPUT} -> {OUTPUT} ===", flush=True)
    clean_candidate()
    clean_municipal_history()
    build_dicionario()
    print("DONE")


if __name__ == "__main__":
    main()
