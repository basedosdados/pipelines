"""Emit per-table columns_json payloads + OL grain map for backend registration."""

import json
import os

# pyrefly: ignore [missing-import]
from schema_spec import TABLE_ORDER, TABLES

OUT = os.path.join(os.path.dirname(__file__), "metadata_payload")
os.makedirs(OUT, exist_ok=True)

# grain columns to link to each table's observation level
OL_GRAIN = {
    "person": ["person_id"],
    "kinship": ["person_id"],
    "office_posting": ["person_id"],
    "association": ["person_id"],
    "address": ["person_id"],
    "office_code": ["office_code"],
    "address_code": ["address_code"],
    "kinship_code": ["kinship_code"],
    "association_code": ["association_code"],
    "dicionario": [],
}


def main():
    index = {}
    # pyrefly: ignore [unknown-name]
    for name in TABLE_ORDER:
        # pyrefly: ignore [unknown-name]
        spec = TABLES[name]
        cols = []
        for c in spec["columns"]:
            cols.append(
                {
                    "name": c["name"],
                    "bigquery_type": c["type"],
                    "description_pt": c["pt"],
                    "description_en": c["en"],
                    "description_es": c["es"],
                    "covered_by_dictionary": c.get("dict", "no") == "yes",
                    "measurement_unit": c.get("unit", ""),
                    "has_sensitive_data": False,
                }
            )
        path = os.path.join(OUT, f"{name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cols, f, ensure_ascii=False)
        index[name] = dict(
            # pyrefly: ignore [unknown-name]
            source=TABLES[name]["source"],
            name_pt=spec["name_pt"],
            name_en=spec["name_en"],
            name_es=spec["name_es"],
            desc_pt=spec["desc_pt"],
            desc_en=spec["desc_en"],
            desc_es=spec["desc_es"],
            n_cols=len(cols),
            ol_grain=OL_GRAIN[name],
        )
        print(f"{name:16} {len(cols):>2} cols -> {path}")
    with open(os.path.join(OUT, "_index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=1)
    print("index written")


if __name__ == "__main__":
    main()
