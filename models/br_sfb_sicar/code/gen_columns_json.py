"""Emit per-table columns_json for backend registration (bulk_upsert_columns),
derived from architecture.py. Includes name, bigquery_type, trilingual
descriptions, measurement_unit, directory_column, covered_by_dictionary,
is_partition."""

import json
import os

import architecture as A  # noqa: N812


def col_json(c):
    # Published column name: the staging snapshot `data` is published as
    # `data_extracao` (see gen_dbt.pub); everything else is identity.
    d = {
        "name": "data_extracao" if c["name"] == "data" else c["name"],
        "bigquery_type": c["type"],
        "description_pt": c["desc_pt"],
        "description_en": c["desc_en"],
        "description_es": c["desc_es"],
        "covered_by_dictionary": c["covered_by_dictionary"] == "yes",
        "has_sensitive_data": False,
    }
    if c["directory"]:
        d["directory_column"] = c["directory"]
    if c["unit"]:
        d["measurement_unit"] = c["unit"]
    return d


def main():
    out = {t: [col_json(c) for c in cols] for t, cols in A.TABLES.items()}
    out["dicionario"] = [col_json(c) for c in A.DICIONARIO]
    path = os.path.join(os.path.dirname(__file__), "columns_json.json")
    with open(path, "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print("wrote", path, "tables:", list(out))
    print("area_imovel cols:", [c["name"] for c in out["area_imovel"]])


if __name__ == "__main__":
    main()
