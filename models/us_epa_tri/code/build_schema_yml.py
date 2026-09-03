"""Generate ``schema.yml`` from the architecture CSVs, tables.json, and the data.

``ignore_values`` for ``not_null_proportion_multiple_columns`` is measured, not
guessed: the set of columns whose non-null share in the cleaned output falls
below the test's threshold. Every column that lands there is listed in
SPARSE_EXPECTED with the reason; an unexpected sparse column fails the build,
because that is how a broken transform hides.

    PYTHONPATH=. uv run python models/us_epa_tri/code/build_schema_yml.py \
        --output-dir ~/Downloads/us_epa_tri_data/output
"""

import argparse
import csv
import glob
import json
from pathlib import Path

import pyarrow.parquet as pq

HERE = Path(__file__).parent
ARCH = HERE / "architecture"
TABLES_JSON = HERE / "tables.json"
OUT = HERE.parent / "schema.yml"
DATASET = "us_epa_tri"
AT_LEAST = 0.05

# Columns allowed below the 5% fill threshold, with the reason.
SPARSE_EXPECTED = {
    "facility": {
        "bia_code": "only facilities on tribal land",
        "tribe_name": "only facilities on tribal land",
        "horizontal_datum": "rarely furnished by the source",
        "foreign_parent_company_name": "only foreign-owned facilities",
        "foreign_parent_company_duns": "only foreign-owned facilities",
        "standardized_foreign_parent_company_name": "only foreign-owned facilities",
    },
    "form": {
        "sic_3": "secondary SIC codes, RY 1987-2005 only",
        "sic_4": "secondary SIC codes, RY 1987-2005 only",
        "sic_5": "secondary SIC codes, RY 1987-2005 only",
        "sic_6": "secondary SIC codes, RY 1987-2005 only",
        "naics_3": "secondary NAICS codes",
        "naics_4": "secondary NAICS codes",
        "naics_5": "secondary NAICS codes",
        "naics_6": "secondary NAICS codes",
        "elemental_metal_included": "collected since RY 2018",
        "production_ratio_type": "collected in recent years only",
    },
    "release": {
        "quantity_grams": "dioxin and dioxin-like compounds only",
    },
}

# Directory relationship tests: column -> (ref model, field, where, tolerance).
RELATIONSHIPS = {
    ("facility", "state"): (
        "br_bd_diretorios_us__state",
        "abbreviation",
        None,
        None,
    ),
    # Connecticut facilities carry the legacy county FIPS (09001-09015); the
    # directory lists the 2022 planning regions instead. Measured 1.5%.
    ("facility", "county_id"): (
        "br_bd_diretorios_us__county",
        "id_county",
        "county_id is not null",
        0.02,
    ),
    ("form", "primary_sic"): (
        "br_bd_diretorios_us__sic",
        "id_sic",
        "primary_sic is not null",
        0.02,
    ),
}
NAICS_VINTAGES = ["2002", "2007", "2012", "2017", "2022"]
# Measured share of primary NAICS codes absent from the vintage directory,
# rounded up; set from build-time measurement (see ONBOARDING_PLAN.md).
NAICS_TOLERANCE = 0.02


def block(text: str, indent: int) -> str:
    """Render a description as a YAML `>` block scalar (colons are safe there)."""
    pad = " " * indent
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 76:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    return ">\n" + "\n".join(pad + ln for ln in lines)


def measure_fill(output_dir: Path, table: str) -> dict[str, float]:
    """Non-null share per column, read from the parquet footers."""
    files = sorted(
        glob.glob(str(output_dir / table / "**" / "*.parquet"), recursive=True)
    )
    files = [f for f in files if not f.endswith("00_header.parquet")]
    if not files:
        return {}
    total = 0
    nulls: dict[str, int] = {}
    names: list[str] = []
    for path in files:
        md = pq.ParquetFile(path).metadata
        schema = md.schema
        if not names:
            names = [schema.column(i).name for i in range(md.num_columns)]
        total += md.num_rows
        for rg in range(md.num_row_groups):
            row_group = md.row_group(rg)
            for i in range(md.num_columns):
                stats = row_group.column(i).statistics
                if stats is None:
                    raise SystemExit(
                        f"{path}: column {names[i]} has no statistics"
                    )
                nulls[names[i]] = nulls.get(names[i], 0) + stats.null_count
    if not total:
        return {}
    return {n: (total - nulls.get(n, 0)) / total for n in names}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, required=True)
    args = ap.parse_args()

    meta = json.loads(TABLES_JSON.read_text(encoding="utf-8"))
    lines = ["---", "version: 2", "models:"]

    for table, tmeta in meta.items():
        with open(ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
            cols = list(csv.DictReader(fh))
        dict_cols = [
            c["name"] for c in cols if c["covered_by_dictionary"] == "yes"
        ]
        fill = measure_fill(args.output_dir, table)
        sparse = sorted(
            c["name"]
            for c in cols
            if c["name"] in fill and fill[c["name"]] < AT_LEAST
        )
        unexpected = [
            c for c in sparse if c not in SPARSE_EXPECTED.get(table, {})
        ]
        if unexpected:
            raise SystemExit(
                f"{table}: unexpected sparse columns {unexpected}: {[(c, round(fill[c], 4)) for c in unexpected]}"
            )

        lines.append(f"  - name: {DATASET}__{table}")
        lines.append("    description: " + block(tmeta["description_pt"], 6))
        lines.append("    tests:")
        key = tmeta["unique_key"]
        lines.append("      - dbt_utils.unique_combination_of_columns:")
        lines.append(f"          combination_of_columns: [{', '.join(key)}]")
        if "year" in key:
            lines.append("          config:")
            lines.append("            where: __most_recent_year_en__")
        lines.append("      - not_null_proportion_multiple_columns:")
        lines.append(f"          at_least: {AT_LEAST}")
        if sparse:
            lines.append(
                "          # Legitimately sparse (measured on the full history):"
            )
            lines.append("          ignore_values:")
            for name in sparse:
                lines.append(
                    f"            - {name}  # {SPARSE_EXPECTED[table][name]}"
                )
        if dict_cols:
            lines.append("      - custom_dictionary_coverage:")
            lines.append("          columns_covered_by_dictionary:")
            for name in dict_cols:
                lines.append(f"            - {name}")
            lines.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
        lines.append("    columns:")
        for c in cols:
            name = c["name"]
            lines.append(f"      - name: {name}")
            lines.append("        description: " + block(c["description"], 10))
            tests = []
            if name in tmeta["not_null"]:
                tests.append("not_null")
            if name == "year":
                lines.append("        tests:")
                lines.append("          - not_null")
                lines.append("          - relationships:")
                lines.append(
                    "              to: ref('br_bd_diretorios_data_tempo__ano')"
                )
                lines.append("              field: ano.ano")
                continue
            rel = RELATIONSHIPS.get((table, name))
            is_naics = table == "form" and name == "primary_naics"
            if rel is None and not tests and not is_naics:
                continue
            lines.append("        tests:")
            for t in tests:
                lines.append(f"          - {t}")
            if rel:
                ref, field, where, tol = rel
                if tol is None:
                    lines.append("          - relationships:")
                    lines.append(f"              to: ref('{ref}')")
                    lines.append(f"              field: {field}")
                else:
                    lines.append("          - custom_relationships:")
                    lines.append(f"              to: ref('{ref}')")
                    lines.append(f"              field: {field}")
                    lines.append(
                        f"              proportion_allowed_failures: {tol}"
                    )
                    if where:
                        lines.append("              config:")
                        lines.append(f"                where: {where}")
            if is_naics:
                for v in NAICS_VINTAGES:
                    lines.append("          - custom_relationships:")
                    lines.append(
                        f"              to: ref('br_bd_diretorios_us__naics_{v}')"
                    )
                    lines.append("              field: id_naics")
                    lines.append(
                        f"              proportion_allowed_failures: {NAICS_TOLERANCE}"
                    )
                    lines.append("              config:")
                    lines.append(
                        f"                where: naics_version = '{v}' and primary_naics is not null"
                    )
        lines.append("")

    OUT.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    print(f"{OUT}: {len(meta)} models")


if __name__ == "__main__":
    main()
