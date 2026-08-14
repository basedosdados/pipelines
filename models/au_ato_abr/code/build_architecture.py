"""Emit architecture CSVs (BD source of truth) from the columns_json + provenance.

One CSV per table, columns in the canonical BD architecture order. Descriptions
are the Portuguese ones from columns_json; EN/ES live in columns_json and are
applied at metadata registration via bulk_upsert_columns.
"""

import csv
import json
from pathlib import Path

HERE = Path(__file__).parent
JSON_DIR = HERE / "columns_json"
OUT_DIR = HERE / "architecture"
OUT_DIR.mkdir(parents=True, exist_ok=True)

HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

# provenance: name -> (original_name, observations)
PROV = {
    "entity": {
        "extraction_date": (
            "ExtractTime",
            "Partition column; snapshot date from the extract header",
        ),
        "abn": ("ABN", ""),
        "abn_status": ("ABN@status", ""),
        "abn_status_from_date": (
            "ABN@ABNStatusFromDate",
            "Sentinel 19000101 mapped to null",
        ),
        "entity_type": ("EntityType/EntityTypeInd", ""),
        "entity_name": (
            "NonIndividualNameText or IndividualName",
            "For individuals assembled as given + family name",
        ),
        "asic_number": ("ASICNumber", ""),
        "asic_number_type": ("ASICNumber@ASICNumberType", ""),
        "gst_status": ("GST@status", ""),
        "gst_status_from_date": (
            "GST@GSTStatusFromDate",
            "Sentinel 19000101 mapped to null",
        ),
        "state_code": (
            "BusinessAddress/AddressDetails/State",
            "Link to br_bd_diretorios_au state/territory directory when available in prod",
        ),
        "postcode": (
            "BusinessAddress/AddressDetails/Postcode",
            "Link to an Australian POA postal-area directory when available",
        ),
        "record_last_updated_date": ("ABR@recordLastUpdatedDate", ""),
        "replaced": ("ABR@replaced", ""),
    },
    "other_name": {
        "extraction_date": (
            "ExtractTime",
            "Partition column; snapshot date from the extract header",
        ),
        "abn": ("ABN", "Foreign key to entity.abn"),
        "name_type": ("OtherEntity/NonIndividualName@type", ""),
        "name": ("OtherEntity/NonIndividualName/NonIndividualNameText", ""),
    },
    "dgr": {
        "extraction_date": (
            "ExtractTime",
            "Partition column; snapshot date from the extract header",
        ),
        "abn": ("ABN", "Foreign key to entity.abn"),
        "dgr_status_from_date": (
            "DGR@DGRStatusFromDate",
            "Sentinel 19000101 mapped to null",
        ),
        "dgr_name": ("DGR/NonIndividualName/NonIndividualNameText", ""),
    },
    "dicionario": {
        c: ("", "")
        for c in [
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ]
    },
}


def main():
    for jf in sorted(JSON_DIR.glob("*.json")):
        table = jf.stem
        cols = json.loads(jf.read_text())
        prov = PROV.get(table, {})
        out = OUT_DIR / f"{table}.csv"
        with open(out, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(HEADER)
            for c in cols:
                orig, obs = prov.get(c["name"], ("", ""))
                w.writerow(
                    [
                        c["name"],
                        c["bigquery_type"],
                        c["description_pt"],
                        "",  # temporal_coverage (same as table)
                        "yes" if c.get("covered_by_dictionary") else "no",
                        "",  # directory_column (none wired yet)
                        "",  # measurement_unit (no numeric quantities in this dataset)
                        "yes" if c.get("has_sensitive_data") else "no",
                        obs,
                        orig,
                    ]
                )
        print(f"wrote {out}  ({len(cols)} cols)")


if __name__ == "__main__":
    main()
