"""Build the `dicionario` table for us_fema_openfema.

Most legends are machine-readable already: OpenFEMA embeds the legal values in
the field description, either behind a "Legal values (value : description):"
marker or as prose like "1 - Slab 2 - Basement". Those are parsed out of the
cached field dictionary. The rest — where FEMA links to a web page instead of
listing values, or where the list is a prose summary of zone families rather
than an enumeration — are hand-written in ``legends.py``.

The build then checks the legend against the values actually present in the
cleaned output and reports any code that occurs in the data but has no label.
That gap is reported rather than hidden: an unlabelled code is a defect in the
dictionary, and silently dropping it would make the table look complete.

    uv run python build_dicionario.py [<output_dir>]
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import glossary  # noqa: E402
import legends  # noqa: E402
import tables as spec  # noqa: E402

OUT = HERE / "dicionario.csv"
HEADER = ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]

# "Legal values (value : description): 0 : $500; 1 : $1,000;" and the prose
# forms "1 - Slab 2 - Basement", "1=single family residence".
_MARKER = re.compile(r"[Ll]egal\s+[Vv]alues?\s*(?:\([^)]*\))?\s*:?", re.I)
_PAIR = re.compile(
    r"(?:^|[;\s])([A-Z0-9]{1,3})\s*[-:=]\s*(.+?)(?=(?:[;]|(?:\s(?:[A-Z0-9]{1,3})\s*[-:=]\s))|$)",
    re.S,
)


def parse_legend(description: str) -> dict[str, str]:
    """Pull `code -> label` pairs out of an OpenFEMA field description."""
    text = re.sub(r"\s+", " ", description or "")
    marker = _MARKER.search(text)
    if marker:
        text = text[marker.end() :]
    out: dict[str, str] = {}
    for code, label in _PAIR.findall(text):
        label = label.strip().strip(";").strip()
        # Cut trailing commentary that is not part of the label.
        label = re.split(r"\s+(?:NOTE|Note:)\s*", label)[0].strip()
        if not label or len(label) > 180:
            continue
        out.setdefault(code.strip(), label)
    return out


def snake(name: str) -> str:
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def source_descriptions() -> dict[str, str]:
    """Harmonised column name -> the source description, for coded columns."""
    meta = json.loads((HERE / "source_metadata.json").read_text())
    out: dict[str, str] = {}
    for field in meta["fields"]:
        raw = snake(field["name"])
        name = spec.HARMONISE.get(raw, raw)
        for cfg in spec.TABLES.values():
            name = cfg["rename"].get(raw, name)
        if name in glossary.CODED:
            out.setdefault(name, field["description"] or "")
    return out


def build() -> tuple[list[dict[str, str]], dict[str, dict[str, str]]]:
    descriptions = source_descriptions()
    resolved: dict[str, dict[str, str]] = {}
    for name in sorted(glossary.CODED):
        # A hand-written legend always wins: it exists precisely because the
        # parsed one is wrong or absent.
        if name in legends.LEGENDS:
            resolved[name] = dict(legends.LEGENDS[name])
            continue
        parsed = parse_legend(descriptions.get(name, ""))
        if not parsed:
            raise SystemExit(
                f"{name}: no legend parsed and none hand-written in legends.py"
            )
        resolved[name] = parsed

    rows: list[dict[str, str]] = []
    for table in spec.TABLES:
        columns = [
            row["name"]
            for row in csv.DictReader(
                (HERE / "architecture" / f"{table}.csv").open()
            )
            if row["covered_by_dictionary"] == "yes"
        ]
        for column in columns:
            for code, label in resolved[column].items():
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": code,
                        "cobertura_temporal": "",
                        "valor": label,
                    }
                )
    return rows, resolved


def check_against_data(
    resolved: dict[str, dict[str, str]], output_dir: Path
) -> int:
    """Report codes present in the cleaned data but missing from the legend."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    gaps = 0
    for table in spec.TABLES:
        directory = output_dir / table
        if not directory.exists():
            print(f"{table:<28} SKIP (not cleaned yet)")
            continue
        columns = [
            row["name"]
            for row in csv.DictReader(
                (HERE / "architecture" / f"{table}.csv").open()
            )
            if row["covered_by_dictionary"] == "yes"
        ]
        files = sorted(directory.glob("year=*/data.parquet"))
        table_data = pa.concat_tables(
            [pq.ParquetFile(f).read(columns=columns) for f in files]
        )
        for column in columns:
            array = table_data[column].combine_chunks()
            observed = {
                v["values"]
                for v in pc.value_counts(array).to_pylist()
                if v["values"] is not None
            }
            missing = sorted(observed - set(resolved[column]))
            if missing:
                gaps += len(missing)
                print(
                    f"  {table}.{column}: {len(missing)} unlabelled "
                    f"code(s): {missing[:12]}"
                )
        print(f"{table:<28} checked {len(columns)} dictionary-covered columns")
    return gaps


def main() -> None:
    rows, resolved = build()
    with OUT.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADER, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(
        f"dicionario.csv: {len(rows)} rows across "
        f"{len({(r['id_tabela'], r['nome_coluna']) for r in rows})} "
        f"table/column pairs\n"
    )

    if len(sys.argv) > 1:
        gaps = check_against_data(resolved, Path(sys.argv[1]).expanduser())
        print("\nUNLABELLED CODES IN DATA:", gaps)


if __name__ == "__main__":
    main()
