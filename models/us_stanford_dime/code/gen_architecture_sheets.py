"""Render the trilingual architecture rows that go into the Drive sheets.

``bulk_upsert_columns`` reads a Google Sheet with the standard architecture
columns plus optional ``description_pt/en/es`` and ``observations_pt/en/es``.
This produces exactly that, joining ``architecture.py`` (structure and English)
to ``i18n.py`` and ``observations_i18n.py`` (the other two languages).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch
from i18n import DESCRIPTIONS
from observations_i18n import OBSERVATIONS

HEADER = [
    "name",
    "bigquery_type",
    "description_pt",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations_pt",
    "observations_en",
    "observations_es",
    "original_name",
]


def rows(table: str) -> list[list[str]]:
    out = [HEADER]
    for c in arch.TABLES[table]:
        name, bq, desc, tcov, cdict, direc, unit, sens, obs, orig = c
        d_pt, d_es = DESCRIPTIONS[desc]
        if obs:
            o_pt, o_es = OBSERVATIONS[obs]
            o_en = obs if obs.endswith(".") else obs + "."
        else:
            o_pt = o_es = o_en = ""
        out.append(
            [
                name,
                bq,
                d_pt,
                desc,
                d_es,
                tcov,
                cdict,
                direc,
                unit,
                sens,
                o_pt,
                o_en,
                o_es,
                orig,
            ]
        )
    return out


if __name__ == "__main__":
    payload = {t: rows(t) for t in arch.TABLES}
    Path("architecture_sheets.json").write_text(
        json.dumps(payload, ensure_ascii=False)
    )
    for t, r in payload.items():
        print(f"{t}: {len(r) - 1} columns")
