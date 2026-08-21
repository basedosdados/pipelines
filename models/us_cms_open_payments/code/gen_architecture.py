"""Emit one architecture table per table, plus the EN/ES description sidecar.

The architecture CSV carries the Portuguese description, because
``upload_columns_from_sheet`` maps the bare ``description`` column to
``description_pt``. English and Spanish go to ``descriptions_en_es.csv`` and
are applied afterwards with ``bulk_upsert_columns``.
"""

import csv

import constants as c
import descriptions
import layout
import naming
import schema

ARCH_COLUMNS = [
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

# Columns CMS only began collecting part way through the series. Everything
# else inherits the table's own coverage, which the empty string denotes.
PARTIAL_COVERAGE = {
    **{
        f"covered_recipient_primary_type_{i}": "2021(1)2025"
        for i in range(2, 7)
    },
    **{f"covered_recipient_specialty_{i}": "2021(1)2025" for i in range(2, 7)},
}
PI_PARTIAL = {
    "covered_recipient_type": "2016(1)2025",
    "covered_recipient_npi": "2015(1)2025",
    **{f"primary_type_{i}": "2016(1)2025" for i in range(2, 7)},
    **{f"specialty_{i}": "2016(1)2025" for i in range(2, 7)},
}
OWNERSHIP_PARTIAL = {"physician_npi": "2015(1)2025"}


def temporal_coverage(table: str, column: str) -> str:
    if table == "research_principal_investigator":
        return PI_PARTIAL.get(column, "")
    if table == "ownership":
        return OWNERSHIP_PARTIAL.get(column, "")
    if table in {"general", "research"}:
        return PARTIAL_COVERAGE.get(column, "")
    return ""


def original_names(table: str) -> dict[str, str]:
    """Data Basis column name -> the CMS column it came from."""
    if table in layout.HEADERS["profile"]:
        return {
            naming.rename_profile(table, s): s
            for s in layout.HEADERS["profile"][table]
        }
    if table == "research_principal_investigator":
        out = {"year": "Program_Year", "record_id": "Record_ID"}
        out["principal_investigator_number"] = "Principal_Investigator_[1-5]_*"
        for src in (
            layout.HEADERS["detail"]["research_2025"]
            + layout.HEADERS["detail"]["research_2015"]
        ):
            split = naming.split_principal_investigator(src)
            if split and split[1] not in out:
                _, rest = src.split("_", 3)[2], src.split("_", 3)[3]
                out[split[1]] = f"Principal_Investigator_[1-5]_{rest}"
        return out
    if table.startswith("summary_"):
        if table == "summary_dashboard":
            return {
                "year": "PY_<year> column header",
                "dashboard_row_number": "Dashboard_Row_Number",
                "metric": "Data_Metrics",
                "value": "PY_<year>",
            }
        out = {
            naming.rename_summary(s): s
            for s in layout.HEADERS["summary"][table]
        }
        return out
    if table == "dicionario":
        return {}
    spec = layout.tables.DETAIL_TABLES[table]
    source = layout.tables._union(
        [f"{spec['kind']}_{y}" for y in spec["years"]]
    )
    return {
        naming.rename(s): s
        for s in source
        if not naming.split_principal_investigator(s)
    }


def rows(table: str) -> list[dict[str, str]]:
    originals = original_names(table)
    out = []
    for column in layout.LAYOUT[table]:
        pt, _, _ = descriptions.describe(table, column)
        out.append(
            {
                "name": column,
                "bigquery_type": schema.bigquery_type(table, column),
                "description": pt,
                "temporal_coverage": temporal_coverage(table, column),
                "covered_by_dictionary": schema.covered_by_dictionary(
                    table, column
                ),
                "directory_column": schema.directory_column(table, column),
                "measurement_unit": schema.measurement_unit(table, column),
                "has_sensitive_data": schema.has_sensitive_data(table, column),
                "observations": schema.observations(table, column),
                "original_name": originals.get(column, ""),
            }
        )
    return out


def main() -> None:
    c.ARCH_DIR.mkdir(parents=True, exist_ok=True)
    sidecar = []
    for table in layout.LAYOUT:
        path = c.ARCH_DIR / f"{table}.csv"
        with open(path, "w", newline="") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=ARCH_COLUMNS, lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(rows(table))
        for column in layout.LAYOUT[table]:
            _pt, en, es = descriptions.describe(table, column)
            sidecar.append(
                {
                    "table": table,
                    "name": column,
                    "description_en": en,
                    "description_es": es,
                }
            )
        print(
            f"{table:38s} {len(layout.LAYOUT[table]):3d} cols -> {path.name}"
        )

    side = c.ARCH_DIR / "descriptions_en_es.csv"
    with open(side, "w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["table", "name", "description_en", "description_es"],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(sidecar)
    print(f"\n{len(layout.LAYOUT)} tables, {len(sidecar)} columns")


if __name__ == "__main__":
    main()
