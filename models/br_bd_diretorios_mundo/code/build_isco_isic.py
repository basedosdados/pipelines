"""Build the ISCO-08 and ISIC Rev.4 directory tables.

Source: the PIAAC Cycle 2 international codebook, which ships both classifications
in full as lookup sheets (`ISCO08`, `ISIC4`). Each sheet is a single mixed-level
list -- ISCO-08 carries 1- to 4-digit codes, ISIC Rev.4 a letter section plus 2- to
4-digit codes -- so one table per classification with a `nivel` column and the parent
codes covers every level. The `*_2digit` sheets in the codebook are strict subsets of
the full lists and need no table of their own.

Titles are published by the OECD in English only. `nome_en` is therefore the single
name column; Portuguese and Spanish titles are not fabricated here.

Writes all-STRING partition-free parquet, per bigquery-conventions.md.
"""

from __future__ import annotations

import os
from pathlib import Path

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq

DATA_ROOT = Path(
    os.environ.get(
        "PIAAC_DATA_ROOT", Path.home() / "Downloads" / "world_oecd_piaac_data"
    )
)
CODEBOOK_CY2 = (
    DATA_ROOT / "input" / "docs" / "piaac-cy2-international-codebook.xlsx"
)
OUTPUT_ROOT = DATA_ROOT / "output" / "diretorios"

ISCO_LEVELS = {
    1: "grande_grupo",
    2: "subgrupo_principal",
    3: "subgrupo",
    4: "grupo_base",
}
ISIC_LEVELS = {1: "secao", 2: "divisao", 3: "grupo", 4: "classe"}


def read_lookup(sheet: str) -> list[tuple[str, str]]:
    """Return (code, label) pairs from a codebook lookup sheet, header row skipped."""
    workbook = openpyxl.load_workbook(
        CODEBOOK_CY2, read_only=True, data_only=True
    )
    rows = []
    for code, label, *_ in workbook[sheet].iter_rows(
        min_row=2, values_only=True
    ):
        if code is None:
            continue
        rows.append((str(code).strip(), str(label).strip()))
    workbook.close()
    return rows


def build_isco() -> list[dict]:
    """ISCO-08 parents are code prefixes, so each level derives from the code itself."""
    records = []
    for code, label in read_lookup("ISCO08"):
        records.append(
            {
                "id_isco_08": code,
                "id_isco_08_grande_grupo": code[:1],
                "id_isco_08_subgrupo_principal": code[:2]
                if len(code) >= 2
                else None,
                "id_isco_08_subgrupo": code[:3] if len(code) >= 3 else None,
                "nivel": ISCO_LEVELS[len(code)],
                "nome_en": label,
            }
        )
    return records


def build_isic() -> list[dict]:
    """ISIC sections are letters, so the section of a division cannot be read off the
    code (A covers 01-03, B covers 05-09). The sheet is in hierarchical order, so the
    most recently seen letter is the current section."""
    records = []
    section = None
    for code, label in read_lookup("ISIC4"):
        if code.isalpha():
            section, nivel = code, ISIC_LEVELS[1]
        else:
            nivel = ISIC_LEVELS[len(code)]
        records.append(
            {
                "id_isic_4": code,
                "id_isic_4_secao": section,
                "id_isic_4_divisao": code[:2] if code[:2].isdigit() else None,
                "id_isic_4_grupo": code[:3] if len(code) >= 3 else None,
                "nivel": nivel,
                "nome_en": label,
            }
        )
    return records


def write_table(records: list[dict], table_slug: str) -> Path:
    """Write one all-STRING parquet file with a stable column order."""
    columns = list(records[0])
    schema = pa.schema([(name, pa.string()) for name in columns])
    table = pa.Table.from_pydict(
        {
            name: pa.array([r[name] for r in records], type=pa.string())
            for name in columns
        },
        schema=schema,
    )
    destination = OUTPUT_ROOT / table_slug
    destination.mkdir(parents=True, exist_ok=True)
    path = destination / "data.parquet"
    pq.write_table(table, path, compression="snappy")
    return path


def main() -> None:
    for table_slug, builder in (
        ("isco_08", build_isco),
        ("isic_4", build_isic),
    ):
        records = builder()
        path = write_table(records, table_slug)
        levels = {}
        for record in records:
            levels[record["nivel"]] = levels.get(record["nivel"], 0) + 1
        codes = [r[f"id_{table_slug}"] for r in records]
        assert len(codes) == len(set(codes)), f"{table_slug}: duplicate codes"
        print(f"{table_slug}: {len(records)} rows -> {path}")
        print(f"  levels: {levels}")


if __name__ == "__main__":
    main()
