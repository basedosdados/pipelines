"""Clean the Justiça em Números consolidated database into three tables.

The source ships one very wide CSV -- 1,596 rows by 1,314 columns, one row per
tribunal and reference year, one column per statistical indicator. Three
properties of that file drive the shape of the output:

1. Five of the 1,311 "indicators" are not measures at all but categorical
   attributes of the tribunal (its name, seat, region, size and structure
   assessment). They cannot share a numeric value column, so they are split
   into their own table together with ``seq_orgao``.
2. The remaining indicators are numeric but use a comma decimal separator and
   sometimes scientific notation (``2,93070325348584e-06``). They are reshaped
   long, which keeps the table stable when CNJ adds indicators.
3. ``nd``, ``n/a`` and ``Inf`` are missing-value sentinels covering 56% of the
   grid. They become NULL and long rows are dropped rather than materialised.

``ramo_justica`` is part of the key, not a descriptive attribute: CNJ files the
STM under both "Militar da União" and "Superior" and reports different -- and in
18 cells genuinely conflicting -- values under each. Dropping it from the key
silently loses or corrupts those rows.

Parquet output is all-STRING by house convention: the dbt model ``safe_cast``s
each column to its architecture type, and a typed staging table collides with
the Prefect pipeline's own all-STRING upload.
"""

from __future__ import annotations

import csv
import os
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

csv.field_size_limit(sys.maxsize)

DATA_ROOT = Path(
    os.environ.get(
        "BR_CNJ_DATAJUD_DATA",
        Path.home() / "Downloads" / "br_cnj_datajud_data",
    )
)
INPUT_DIR = DATA_ROOT / "input"
OUTPUT_DIR = DATA_ROOT / "output"

# Values the source uses for "not available". "Inf" appears where a ratio
# divides by zero. All become NULL.
SENTINELS = {"nd", "n/a", "na", "inf", "-inf", "nan", ""}

# Columns that describe the tribunal rather than measuring its caseload.
# Mapped to their output names in the `tribunal` table.
ATTRIBUTE_COLUMNS = {
    "dsc_tribunal": "nome_tribunal",
    "uf_sede": "sigla_uf_sede",
    "uf_abrangida": "abrangencia",
    "porte": "porte",
    "estrutura": "estrutura",
    "seq_orgao": "sequencial_orgao",
}

# CNJ writes the same ramo two ways across years.
RAMO_NORMALISATION = {"Militar Uniao": "Militar da União"}

# `uf_sede` is "BR" for courts with national jurisdiction. That is not a UF, so
# it cannot carry the directory foreign key; it becomes NULL.
NON_UF_SEDE = {"BR"}

NUMERIC_RE = re.compile(r"^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$")


def read_source(path: Path) -> tuple[list[str], list[list[str]]]:
    text = path.read_text(encoding="latin-1")
    rows = list(csv.reader(text.splitlines(), delimiter=";"))
    return [h.strip() for h in rows[0]], rows[1:]


def clean_value(raw: str) -> str | None:
    """Return a canonical numeric string, or None for a sentinel."""
    value = raw.strip()
    if value.lower() in SENTINELS:
        return None
    # The source uses a comma decimal separator and never a thousands
    # separator (verified: zero values contain a dot).
    value = value.replace(",", ".")
    if not NUMERIC_RE.match(value):
        return None
    return value


def build(
    input_dir: Path = INPUT_DIR, output_dir: Path = OUTPUT_DIR
) -> dict[str, int]:
    jn_files = sorted(input_dir.glob("JN_*.csv"))
    if not jn_files:
        raise FileNotFoundError(
            f"no JN_*.csv in {input_dir}; run download.py first"
        )
    header, rows = read_source(jn_files[-1])

    lower = [h.lower() for h in header]
    idx = {name: lower.index(name) for name in ("ano", "justica", "sigla")}
    attribute_idx = {
        source: lower.index(source)
        for source in ATTRIBUTE_COLUMNS
        if source in lower
    }
    missing = set(ATTRIBUTE_COLUMNS) - set(attribute_idx)
    if missing:
        raise RuntimeError(
            f"expected attribute columns absent from source: {missing}"
        )

    # Every remaining column is a measured indicator.
    indicator_idx = [
        j
        for j, name in enumerate(lower)
        if j not in attribute_idx.values() and name not in idx
    ]

    tribunal_rows: list[dict[str, str | None]] = []
    fact_rows: list[dict[str, str | None]] = []
    dropped = 0

    for row in rows:
        ano = row[idx["ano"]].strip()
        sigla = row[idx["sigla"]].strip()
        ramo = RAMO_NORMALISATION.get(
            row[idx["justica"]].strip(), row[idx["justica"]].strip()
        )

        attributes: dict[str, str | None] = {
            "ano": ano,
            "sigla_tribunal": sigla,
            "ramo_justica": ramo,
        }
        for source, target in ATTRIBUTE_COLUMNS.items():
            value = row[attribute_idx[source]].strip()
            if value.lower() in SENTINELS or (
                target == "sigla_uf_sede" and value in NON_UF_SEDE
            ):
                value = None
            attributes[target] = value
        tribunal_rows.append(attributes)

        for j in indicator_idx:
            value = clean_value(row[j])
            if value is None:
                dropped += 1
                continue
            fact_rows.append(
                {
                    "ano": ano,
                    "sigla_tribunal": sigla,
                    "ramo_justica": ramo,
                    "sigla_indicador": lower[j],
                    "valor": value,
                }
            )

    dictionary_rows = build_dictionary(
        input_dir, set(lower) - set(attribute_idx) - set(idx)
    )

    counts = {
        "tribunal_ano": write_partitioned(
            fact_rows, output_dir / "tribunal_ano"
        ),
        "tribunal": write_partitioned(tribunal_rows, output_dir / "tribunal"),
        "dicionario": write_flat(dictionary_rows, output_dir / "dicionario"),
    }
    counts["_sentinels_dropped"] = dropped
    return counts


def build_dictionary(
    input_dir: Path, indicator_names: set[str]
) -> list[dict[str, str]]:
    """Map every indicator code to the label CNJ publishes for it."""
    path = input_dir / "Variaveis.csv"
    _header, rows = read_source(path)
    labels = {r[0].strip().lower(): r[1].strip() for r in rows if len(r) >= 2}

    uncovered = indicator_names - set(labels)
    if uncovered:
        raise RuntimeError(
            f"{len(uncovered)} indicators have no entry in Variaveis.csv: "
            f"{sorted(uncovered)[:10]}"
        )

    return [
        {
            "id_tabela": "tribunal_ano",
            "nome_coluna": "sigla_indicador",
            "chave": code,
            "cobertura_temporal": "",
            "valor": labels[code],
        }
        for code in sorted(indicator_names)
    ]


def _table(
    records: Sequence[Mapping[str, str | None]], columns: list[str]
) -> pa.Table:
    """Build an all-STRING Arrow table with a stable column order."""
    schema = pa.schema([(name, pa.string()) for name in columns])
    return pa.Table.from_pydict(
        {name: [r.get(name) for r in records] for name in columns},
        schema=schema,
    )


def write_partitioned(records: list[dict[str, str | None]], root: Path) -> int:
    columns = [c for c in records[0] if c != "ano"]
    root.mkdir(parents=True, exist_ok=True)
    by_year: dict[str, list[dict[str, str | None]]] = {}
    for record in records:
        ano = record["ano"]
        if ano is None:
            raise RuntimeError("row without a reference year")
        by_year.setdefault(ano, []).append(record)

    for ano, chunk in sorted(by_year.items()):
        # An empty partition makes the staging header infer the wrong types.
        if not chunk:
            raise RuntimeError(f"empty partition for ano={ano}")
        directory = root / f"ano={ano}"
        directory.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            _table(chunk, columns),
            directory / "data.parquet",
            compression="snappy",
        )
    return len(records)


def write_flat(records: Sequence[Mapping[str, str]], root: Path) -> int:
    root.mkdir(parents=True, exist_ok=True)
    columns = list(records[0])
    pq.write_table(
        _table(records, columns), root / "data.parquet", compression="snappy"
    )
    return len(records)


if __name__ == "__main__":
    for table, count in build().items():
        print(f"{table:24s} {count:>10,}")
