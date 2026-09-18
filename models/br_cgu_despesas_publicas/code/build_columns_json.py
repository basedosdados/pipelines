"""Generate the bulk_upsert_columns payloads for br_cgu_despesas_publicas.

Usage:
    uv run python models/br_cgu_despesas_publicas/code/build_columns_json.py

Reads the per-table specs via ``_specs.py`` and writes
``columns_json/<table>.json``. The architecture CSV carries only the Portuguese
description, so the EN/ES text lives in the specs rather than being retyped at
registration time. Both artifacts come from the same source and cannot drift.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# _specs.py is a sibling module reached through the sys.path insert above,
# which pyrefly cannot follow when it checks the project as a whole.
# pyrefly: ignore [missing-import]
from _specs import TABLES

OUT_DIR = Path(__file__).resolve().parent / "columns_json"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for table, (cols, _period) in TABLES.items():
        payload = [
            {
                "name": c["name"],
                "bigquery_type": c["bigquery_type"],
                "description_pt": c["description_pt"],
                "description_en": c["description_en"],
                "description_es": c["description_es"],
                "covered_by_dictionary": False,
                "has_sensitive_data": c["has_sensitive_data"] == "yes",
                "directory_column": c["directory_column"],
                "measurement_unit": c["measurement_unit"],
                "observations_pt": c["observations_pt"],
            }
            for c in cols
        ]
        path = OUT_DIR / f"{table}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
        print(f"wrote {path} ({len(payload)} columns)")


if __name__ == "__main__":
    main()
