"""Write the architecture CSVs for us_cms_hcris — the schema source of truth on disk.

    python gen_architecture.py

One CSV per table under ``architecture/``, in Data Basis architecture-sheet
column order. Everything downstream reads these files: ``gen_dbt.py`` writes the
dbt models and ``schema.yml`` from them, and ``register.py`` builds the backend
column payloads from them. Edit the column definitions in ``schema.py``, never
the CSVs.

``temporal_coverage`` is taken from ``measured.json`` where that file exists, so
the published coverage of each mapped measure is the span the 538 million
values actually show rather than the span the published mappings claim. Written
by ``verify_measures.py``; absent on a first run, before any parquet exists.
"""

import csv
import json
from pathlib import Path

from codes import CODES, COVERED
from schema import TABLES, Col

CODE_DIR = Path(__file__).resolve().parent
OUT = CODE_DIR / "architecture"
MEASURED = CODE_DIR / "measured.json"

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

# Coverage of the tables as a whole, from the cleaned parquet.
TABLE_COVERAGE = {
    "report": "1996(1)2026",
    "report_value": "1996(1)2026",
    "hospital_financial": "1996(1)2026",
    "dicionario": "",
}


def measure_coverage(name: str, measured: dict) -> str:
    """Render one measure's observed temporal coverage.

    Args:
        name: Measure name.
        measured: Parsed ``measured.json``.

    Returns:
        ``START(1)END`` spanning every form version the measure appears on, or
        an empty string when the measure is not one of the mapped ones or the
        file has not been written yet.
    """
    row = measured.get(name)
    if not row:
        return ""
    years = [
        v[key]
        for v in row.values()
        for key in ("year_min", "year_max")
        if v.get(key) is not None
    ]
    if not years:
        return ""
    return f"{min(int(y) for y in years)}(1){max(int(y) for y in years)}"


def dictionary_columns(table: str) -> set[str]:
    """Names of the table's dictionary-covered columns."""
    return set(COVERED.get(table, []))


def row_for(col: Col, table: str, measured: dict) -> dict[str, str]:
    """Render one column definition as an architecture-sheet row.

    Args:
        col: The column definition.
        table: Table the column belongs to.
        measured: Parsed ``measured.json``.

    Returns:
        A dict keyed by :data:`HEADER`.
    """
    covered = col.name in dictionary_columns(table)
    coverage = measure_coverage(col.name, measured) or TABLE_COVERAGE.get(
        table, ""
    )
    # A column whose coverage equals the table's is left blank, per the Data
    # Basis convention that an empty temporal_coverage means "same as table".
    if coverage == TABLE_COVERAGE.get(table, ""):
        coverage = ""
    return {
        "name": col.name,
        "bigquery_type": col.type,
        "description_pt": col.pt,
        "description_en": col.en,
        "description_es": col.es,
        "temporal_coverage": coverage,
        "covered_by_dictionary": "yes" if covered else "no",
        "directory_column": col.directory,
        "measurement_unit": col.unit,
        "has_sensitive_data": "no",
        "observations_pt": col.obs_pt,
        "observations_en": col.obs,
        "observations_es": col.obs_es,
        "original_name": col.original,
    }


def main() -> None:
    """Write one architecture CSV per table."""
    measured = json.loads(MEASURED.read_text()) if MEASURED.exists() else {}
    if not measured:
        print(
            "measured.json absent — temporal_coverage falls back to the table's"
        )
    OUT.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        path = OUT / f"{table}.csv"
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=HEADER)
            writer.writeheader()
            for col in cols:
                writer.writerow(row_for(col, table, measured))
        print(
            f"{table:<20} {len(cols):>3} columns -> {path.relative_to(CODE_DIR)}"
        )

    # Every column that claims dictionary coverage must have a code list, and
    # every code list must belong to a column that claims coverage. Either half
    # failing makes the dbt custom_dictionary_coverage test fail at build time
    # instead of here.
    for table, cols in TABLES.items():
        declared = {
            c.name for c in cols if c.name in dictionary_columns(table)
        }
        marked = {c.name for c in cols if c.dictionary} & {
            c.name for c in cols
        }
        missing = declared - set(CODES)
        assert not missing, (
            f"{table}: dictionary-covered but no code list: {missing}"
        )
        drift = marked ^ declared
        assert not drift, (
            f"{table}: codes.COVERED and schema.py disagree on {drift}"
        )
    n_codes = sum(len(CODES[c]) for cols in COVERED.values() for c in cols)
    print(
        f"dicionario will hold {n_codes} rows across {len(TABLES) - 1} tables"
    )


if __name__ == "__main__":
    main()
