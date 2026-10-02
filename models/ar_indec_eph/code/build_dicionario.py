"""Build the dicionario table: every coded column's code -> Spanish label.

Two sources, merged in this order of precedence:

1. The Stata value labels of the 2003-2015 era (.dta files). Authoritative,
   because they come from the data itself.
2. INDEC's record-layout PDF, which is the only source for the columns the TXT
   era introduced -- including the roughly 70 added by the 2023 Q4 redesign.
3. overrides.json, for coded columns neither source documents (MAS_500).

Where both sources give a label for the same code, the Stata label wins and the
PDF label is discarded, because the PDF is known to misattribute some codes: it
prints the COMPONENTE codes 51 and 71 underneath NRO_HOGAR. NRO_HOGAR is
excluded here for that reason.

Codes are emitted zero-padded to the same canonical width the cleaning transform
applies to the column, so a dicionario key actually joins to the stored value.
"""

import csv
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from models.ar_indec_eph.code.build_architecture import NEVER_DICTIONARY
from models.ar_indec_eph.code.constants import (
    ARCH_DIR,
    CODE_DIR,
    OUTPUT_DIR,
    TABLES,
)
from models.ar_indec_eph.code.parse_registro import lookup

COLUMNS = ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]


def load(name: str):
    return json.loads((CODE_DIR / name).read_text(encoding="utf-8"))


def main() -> int:
    value_labels = load("value_labels.json")
    registro = load("registro_parsed.json")
    overrides = load("overrides.json")
    pad = load("pad_widths.json")["widths"]
    dict_over = overrides.get("value_labels") or {}

    rows: list[dict] = []
    stats = {
        t: {"columns": 0, "from_stata": 0, "from_pdf": 0, "from_override": 0}
        for t in TABLES
    }

    for table in TABLES:
        with open(ARCH_DIR / f"{table}.csv", encoding="utf-8") as handle:
            arch = list(csv.DictReader(handle))
        widths = pad.get(table, {})
        for row in arch:
            if row["covered_by_dictionary"] != "yes":
                continue
            src, name = row["original_name"], row["name"]
            if src in NEVER_DICTIONARY:
                continue
            merged: dict[str, str] = {}
            origin: dict[str, str] = {}

            pdf = (lookup(registro[table], src) or {}).get("values") or {}
            for code, label in pdf.items():
                merged[code] = label
                origin[code] = "pdf"
            # Stata wins on conflict.
            for code, label in (value_labels[table].get(src) or {}).items():
                merged[code] = label
                origin[code] = "stata"
            for code, label in (dict_over.get(src) or {}).items():
                if code.startswith("_"):
                    continue
                merged[code] = label
                origin[code] = "override"

            if not merged:
                continue
            stats[table]["columns"] += 1
            width = widths.get(src)
            for code, label in merged.items():
                key = code
                if width and key.isdigit() and len(key) < width:
                    key = key.zfill(width)
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": name,
                        "chave": key,
                        "cobertura_temporal": row["temporal_coverage"],
                        "valor": label,
                    }
                )
                stats[table][f"from_{origin[code]}"] += 1

    frame = pd.DataFrame(rows, columns=COLUMNS)
    before = len(frame)
    frame = frame.drop_duplicates(subset=["id_tabela", "nome_coluna", "chave"])
    frame = frame.sort_values(
        ["id_tabela", "nome_coluna", "chave"]
    ).reset_index(drop=True)

    dest = OUTPUT_DIR / "dicionario"
    dest.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in COLUMNS])
    arrow = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
    pq.write_table(arrow, dest / "data.parquet", compression="snappy")

    print(
        f"dicionario: {len(frame)} rows ({before - len(frame)} duplicate keys dropped)"
    )
    for table in TABLES:
        s = stats[table]
        print(
            f"  {table}: {s['columns']} coded columns, "
            f"{s['from_stata']} labels from Stata, {s['from_pdf']} from the PDF, "
            f"{s['from_override']} from overrides"
        )
    per_table = frame.groupby("id_tabela")["nome_coluna"].nunique().to_dict()
    print(f"  distinct columns per table: {per_table}")
    print(f"  written to {dest / 'data.parquet'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
