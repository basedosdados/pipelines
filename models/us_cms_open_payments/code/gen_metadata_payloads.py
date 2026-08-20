"""Build the columns_json payload bulk_upsert_columns expects, per table.

bulk_upsert_columns matches by column name and writes bigquery_type, so the
whole 651-column registration is 23 calls with no Google Sheet in the loop.
The payload is derived from the layout and never stored: ``register_metadata``
builds it in memory, so there is no committed copy to drift from its source.
"""

import descriptions
import gen_architecture
import layout
import schema


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
