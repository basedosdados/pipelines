"""Convert Paraíba NDJSON to all-STRING staging parquet.

PB arrives as JSON from a REST API rather than as delimited text, so none of the
encoding, quoting or ragged-row defects that dominate the other states apply here --
the API emits well-formed UTF-8 JSON and nulls are explicit rather than omitted.

Two things still need care:

* **`participantes` and `documentos` are JSON STRINGS, not nested arrays.** Iterating
  the raw value walks characters, and a length check reports the string length (240)
  rather than the record count (1). `participantes` is exploded into its own staging
  table here; `documentos` is left as the string it is, since nothing models it.
* **The field set is asserted, not unioned.** `union_by_name` over a changed schema is
  what left 1,031,326 PE rows present and entirely NULL, and JSON makes that easier to
  do by accident than CSV does.

Staging is all-STRING by house convention, and it must be all-STRING here specifically:
the recurring-pipeline upload path stringifies its header, so a typed external table
left behind by onboarding collides with the pipeline's later overwrite. See
.claude/rules/prefect-pipeline-conventions.md, "Staging parquet must be all-STRING".
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    INPUT_DIR,
    OUTPUT_DIR,
    PB_COMPRAS_ENDPOINTS,
    PB_DESPESA_ENDPOINTS,
    normalise_column,
)

PB_INPUT = INPUT_DIR / "pb"
PARTICIPANTE_TABLE = "pb_participante"

# Fields of one exploded participant record, in the order the source lists them.
PARTICIPANTE_FIELDS = (
    "lote",
    "item",
    "quantidade",
    "cnpj",
    "razao_social",
    "nome_fantasia",
    "valor_ofertado",
    "valor_licitado",
    "valor_total_licitado",
)


def _recorded_rows(source: Path) -> int | None:
    meta = source.with_suffix(".json")
    if not meta.exists():
        return None
    return json.loads(meta.read_text()).get("rows")


def _fields(source: Path) -> list[str]:
    """Field names of the first record, in order."""
    with source.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                return list(json.loads(line))
    return []


def clean_table(
    con: duckdb.DuckDBPyConnection, table: str, only_year: int | None
) -> int:
    sources = sorted((PB_INPUT / table).glob(f"{table}_*.ndjson"))
    if only_year is not None:
        sources = [
            p
            for p in sources
            if p.stem.rsplit("_", 1)[-1].startswith(str(only_year))
        ]
    if not sources:
        print(f"  {table}: no input files", flush=True)
        return 0

    dest_dir = OUTPUT_DIR / table
    dest_dir.mkdir(parents=True, exist_ok=True)
    reference: list[str] | None = None
    total = 0

    for source in sources:
        fields = _fields(source)
        if not fields:
            print(f"    {source.name}: empty, skipped")
            continue
        if reference is None:
            reference = fields
        elif fields != reference:
            missing = [c for c in reference if c not in fields]
            extra = [c for c in fields if c not in reference]
            raise SystemExit(
                f"{source.name}: field set differs from the reference "
                f"(missing={missing}; extra={extra}). Unioning these would leave one "
                f"schema's columns NULL for every row of the other -- the PE failure. "
                f"Resolve deliberately before continuing."
            )

        # `CAST`, not `safe_cast`: this runs in duckdb, where `safe_cast` does not
        # exist. Every source value is a JSON scalar, so the cast to VARCHAR is total.
        projection = ", ".join(
            f'CAST("{f}" AS VARCHAR) AS "{normalise_column(f)}"'
            for f in fields
        )
        out_path = dest_dir / f"data_{source.stem.rsplit('_', 1)[-1]}.parquet"
        rel = (
            f"read_json('{source}', format='newline_delimited', "
            f"maximum_object_size=20000000)"
        )
        con.execute(
            f"COPY (SELECT {projection} FROM {rel}) TO '{out_path}' "
            f"(FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]

        want = _recorded_rows(source)
        if want is not None and n != want:
            out_path.unlink(missing_ok=True)
            raise SystemExit(
                f"{source.name}: parquet has {n:,} rows but the API reported {want:,}"
            )
        if n == 0:
            # An empty first partition makes dump_header infer INTEGER for every column
            # and poisons the staging schema.
            out_path.unlink()
            continue
        total += n
        print(f"    {source.name}: {n:,} rows", flush=True)

    print(f"  == {table}: {total:,} rows", flush=True)
    return total


def explode_participantes(
    con: duckdb.DuckDBPyConnection, only_year: int | None
) -> int:
    """One row per participant, out of the JSON string on each contratação."""
    sources = sorted(
        (PB_INPUT / "pb_contratacao").glob("pb_contratacao_*.ndjson")
    )
    if only_year is not None:
        sources = [
            p
            for p in sources
            if p.stem.rsplit("_", 1)[-1].startswith(str(only_year))
        ]
    if not sources:
        return 0

    dest_dir = OUTPUT_DIR / PARTICIPANTE_TABLE
    dest_dir.mkdir(parents=True, exist_ok=True)
    total = 0

    for source in sources:
        year = source.stem.rsplit("_", 1)[-1]
        rows: list[dict] = []
        with source.open(encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                rec = json.loads(line)
                raw = rec.get("participantes")
                if not raw:
                    continue
                try:
                    # A JSON string, not a list -- see the module docstring.
                    parts = json.loads(raw) if isinstance(raw, str) else raw
                except json.JSONDecodeError as exc:
                    raise SystemExit(
                        f"{source.name}: `participantes` is neither a list nor valid "
                        f"JSON for processo {rec.get('numeroProcesso')!r}"
                    ) from exc
                for p in parts or []:
                    rows.append(
                        {
                            "ano": str(year),
                            "numero_processo": str(
                                rec.get("numeroProcesso") or ""
                            ),
                            "numero_licitacao": str(
                                rec.get("numeroLicitacao") or ""
                            ),
                            **{
                                f: ("" if p.get(f) is None else str(p.get(f)))
                                for f in PARTICIPANTE_FIELDS
                            },
                        }
                    )
        if not rows:
            continue
        out_path = dest_dir / f"data_{year}.parquet"
        tmp = dest_dir / f".{year}.ndjson"
        with tmp.open("w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        try:
            con.execute(
                f"COPY (SELECT * FROM read_json('{tmp}', format='newline_delimited', "
                f"maximum_object_size=20000000)) TO '{out_path}' "
                f"(FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
        finally:
            tmp.unlink(missing_ok=True)
        total += len(rows)
        print(
            f"    {PARTICIPANTE_TABLE} {year}: {len(rows):,} rows", flush=True
        )

    print(f"  == {PARTICIPANTE_TABLE}: {total:,} rows", flush=True)
    return total


def main(only_year: int | None = None) -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    grand: dict[str, int] = {}
    for table in list(PB_DESPESA_ENDPOINTS.values()) + list(
        PB_COMPRAS_ENDPOINTS.values()
    ):
        print(f"  {table}", flush=True)
        grand[table] = clean_table(con, table, only_year)
    grand[PARTICIPANTE_TABLE] = explode_participantes(con, only_year)
    for table, n in grand.items():
        print(f"{table:<22} {n:>12,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int)
    args = parser.parse_args()
    main(only_year=args.year)
