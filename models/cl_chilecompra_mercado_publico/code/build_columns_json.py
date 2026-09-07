"""Emit the bulk_upsert_columns payload for each table, from architecture + i18n.

Keeping this in one place means the trilingual descriptions registered in the backend
are derived from the same architecture that drives the dbt models, rather than typed
twice. A description present in the architecture but missing a translation raises here
instead of registering Portuguese-only, which is how 3,022 production columns across
the project ended up PT-only.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from descriptions_i18n import DESCRIPTIONS, OBSERVATIONS  # noqa: E402

TABLES = ["orden_compra_item", "licitacion_item", "licitacion_oferta"]


def build(table: str) -> list[dict]:
    with (HERE / "architecture" / f"{table}.csv").open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    out = []
    for r in rows:
        desc = r["description"].strip()
        if desc not in DESCRIPTIONS:
            raise SystemExit(
                f"{table}.{r['name']}: no translation for {desc!r}"
            )
        en, es = DESCRIPTIONS[desc]
        col = {
            "name": r["name"],
            "bigquery_type": r["bigquery_type"],
            "description_pt": desc,
            "description_en": en,
            "description_es": es,
        }
        # Only non-default flags are emitted: the endpoint writes just the fields
        # present, and a new column defaults to false for both, so sending false on
        # every row only inflates the payload.
        if r["covered_by_dictionary"] == "yes":
            col["covered_by_dictionary"] = True
        if r["has_sensitive_data"] == "yes":
            col["has_sensitive_data"] = True
        if r["directory_column"].strip():
            col["directory_column"] = r["directory_column"].strip()
        if r["measurement_unit"].strip():
            col["measurement_unit"] = r["measurement_unit"].strip()
        obs = r["observations"].strip()
        if obs:
            if obs not in OBSERVATIONS:
                raise SystemExit(
                    f"{table}.{r['name']}: no translation for observation {obs!r}"
                )
            oen, oes = OBSERVATIONS[obs]
            col["observations_pt"] = obs
            col["observations_en"] = oen
            col["observations_es"] = oes
        out.append(col)
    return out


if __name__ == "__main__":
    for table in TABLES:
        cols = build(table)
        path = HERE / f"columns_{table}.json"
        path.write_text(json.dumps(cols, ensure_ascii=False), encoding="utf-8")
        print(
            f"{table}: {len(cols)} columns -> {path.name} ({path.stat().st_size:,} bytes)"
        )
