"""Generate the bulk_upsert_columns payload for br_cgu_despesas_publicas.

Usage:
    uv run python models/br_cgu_despesas_publicas/code/build_columns_json.py

Reads ``_spec.py`` — the trilingual column spec — and writes
``columns_json/execucao.json``. The architecture CSV carries only the Portuguese
description, so the EN/ES text lives here rather than being retyped at
registration time. Both files are generated from the same spec, so they cannot
drift.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# _spec.py is a sibling module reached through the sys.path insert above,
# which pyrefly cannot follow when it checks the project as a whole.
# pyrefly: ignore [missing-import]
from _spec import SPEC

OUT = Path(__file__).resolve().parent / "columns_json" / "execucao.json"


def main() -> None:
    payload = [
        {
            "name": name,
            "bigquery_type": typ,
            "description_pt": pt,
            "description_en": en,
            "description_es": es,
            "covered_by_dictionary": False,
            "has_sensitive_data": False,
            "directory_column": dc,
            "measurement_unit": unit,
            "observations_pt": obs,
        }
        for name, typ, _src, pt, en, es, unit, dc, obs in SPEC
    ]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    print(f"wrote {OUT} ({len(payload)} columns)")


if __name__ == "__main__":
    main()
