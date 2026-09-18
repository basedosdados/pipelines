"""Emit per-table columns_json for bulk_upsert_columns, from architecture_spec.

uv run python gen_columns_json.py     # writes code/columns_json/<slug>.json
"""

import json
import os

# pyrefly: ignore [missing-import]
from architecture_spec import TABLES

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "columns_json")
BQ = {
    "str": "STRING",
    "int": "INT64",
    "float": "FLOAT64",
    "date": "DATE",
    "datetime": "DATETIME",
}


def col_dict(col: tuple) -> dict:
    name, typ, pt, en, es = col[0], col[1], col[2], col[3], col[4]
    opts = col[5] if len(col) > 5 else {}
    d = {
        "name": name,
        "description_pt": pt,
        "description_en": en,
        "description_es": es,
        "bigquery_type": BQ[typ],
        "has_sensitive_data": bool(opts.get("sensitive", False)),
        "covered_by_dictionary": bool(opts.get("dict", False)),
    }
    if opts.get("dir"):
        d["directory_column"] = opts["dir"]
    if opts.get("unit"):
        d["measurement_unit"] = opts["unit"]
    return d


def main() -> None:
    os.makedirs(OUT, exist_ok=True)
    for slug, spec in TABLES.items():
        cols = [col_dict(c) for c in spec["cols"]]
        with open(
            os.path.join(OUT, f"{slug}.json"), "w", encoding="utf-8"
        ) as f:
            json.dump(cols, f, ensure_ascii=False, indent=1)
    print(f"wrote columns_json for {len(TABLES)} tables")


if __name__ == "__main__":
    main()
