"""Emit the columns_json payload for bulk_upsert_columns, one file per table.

bulk_upsert_columns matches by column name and writes bigquery_type, so the
whole 651-column registration is 23 calls with no Google Sheet in the loop.

    uv run python gen_metadata_payloads.py
"""

import json

import constants as c
import descriptions
import gen_architecture
import layout
import schema

OUT_DIR = c.ARCH_DIR / "payloads"


def payload(table: str) -> list[dict]:
    originals = gen_architecture.original_names(table)
    out = []
    for column in layout.LAYOUT[table]:
        pt, en, es = descriptions.describe(table, column)
        entry = {
            "name": column,
            "bigquery_type": schema.bigquery_type(table, column),
            "description_pt": pt,
            "description_en": en,
            "description_es": es,
            "covered_by_dictionary": schema.covered_by_dictionary(
                table, column
            )
            == "yes",
            "has_sensitive_data": schema.has_sensitive_data(table, column)
            == "yes",
        }
        directory = schema.directory_column(table, column)
        if directory:
            entry["directory_column"] = directory
        unit = schema.measurement_unit(table, column)
        if unit:
            entry["measurement_unit"] = unit
        note = schema.observations(table, column)
        original = originals.get(column, "")
        provenance = (
            f"Coluna de origem no CMS: {original}." if original else ""
        )
        combined = " ".join(x for x in (note, provenance) if x)
        if combined:
            entry["observations"] = combined
        coverage = gen_architecture.temporal_coverage(table, column)
        if coverage:
            entry["temporal_coverage"] = coverage
        out.append(entry)
    return out


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    total = 0
    for table in layout.LAYOUT:
        rows = payload(table)
        path = OUT_DIR / f"{table}.json"
        with open(path, "w") as fh:
            json.dump(rows, fh, ensure_ascii=False, indent=1)
        total += len(rows)
        print(f"{table:38s} {len(rows):3d} columns -> {path.name}")
    print(f"\n{total} columns across {len(layout.LAYOUT)} tables")
