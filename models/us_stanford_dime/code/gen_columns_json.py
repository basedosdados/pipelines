"""Render the ``columns_json`` payload for ``bulk_upsert_columns``.

The Drive sheets stay private, so the backend cannot fetch them over
``architecture_url`` — it returns HTTP 401. Passing the columns inline avoids
making org Drive content link-readable just to register metadata, and produces
exactly the same records.

    uv run python -m models.us_stanford_dime.code.gen_columns_json <table>
"""

from __future__ import annotations

import json
import sys

from models.us_stanford_dime.code import architecture as arch
from models.us_stanford_dime.code.i18n import DESCRIPTIONS
from models.us_stanford_dime.code.observations_i18n import OBSERVATIONS


def payload(table: str) -> list[dict]:
    out = []
    for c in arch.TABLES[table]:
        # pyrefly: ignore [bad-unpacking]
        name, bq, desc, _tcov, cdict, direc, unit, sens, obs, _orig = c
        d_pt, d_es = DESCRIPTIONS[desc]
        row = {
            "name": name,
            "bigquery_type": bq,
            "description_pt": d_pt,
            "description_en": desc,
            "description_es": d_es,
            "covered_by_dictionary": cdict == "yes",
            "has_sensitive_data": sens == "yes",
        }
        if direc:
            row["directory_column"] = direc
        if unit:
            row["measurement_unit"] = unit
        if obs:
            o_pt, o_es = OBSERVATIONS[obs]
            row["observations_pt"] = o_pt
            row["observations_en"] = obs if obs.endswith(".") else obs + "."
            row["observations_es"] = o_es
        out.append(row)
    return out


if __name__ == "__main__":
    print(json.dumps(payload(sys.argv[1]), ensure_ascii=False))
