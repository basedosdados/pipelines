"""Generate ``schema.yml`` from the architecture CSVs, tables.json, and the data.

``ignore_values`` for ``not_null_proportion_multiple_columns`` is not guessed: it
is the measured set of columns whose non-null share in the cleaned output falls
below the test's threshold. Run after a full clean, pointing at the output dir.

    python build_schema_yml.py --output-dir ~/Downloads/us_hhs_nppes_data/output
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
DATASET = "us_hhs_nppes"
AT_LEAST = 0.05

# The source repeats byte-identical rows in these files; the models collapse
# them with SELECT DISTINCT, so the uniqueness key is checked post-dedup.
DISTINCT = {"practice_location", "endpoint"}


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
    """Non-null share per column, read from the cleaned parquet."""
    files = sorted(
        glob.glob(str(output_dir / table / "**" / "*.parquet"), recursive=True)
    )
    files = [f for f in files if not f.endswith("00_header.parquet")]
    if not files:
        return {}
    # Read null counts out of the parquet footers rather than the data: the
    # tables run to tens of millions of rows and the statistics are exact.
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
                        f"{path}: column {names[i]} has no statistics; "
                        "cannot measure fill rate from metadata"
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
        # extraction_date lives in the partition path, so it is absent from the
        # parquet body; it is never sparse.
        sparse = sorted(
            c["name"]
            for c in cols
            if c["name"] in fill and fill[c["name"]] < AT_LEAST
        )

        lines.append(f"  - name: {DATASET}__{table}")
        lines.append("    description: " + block(tmeta["description_pt"], 6))
        lines.append("    tests:")
        key = tmeta["unique_key"]
        if table in DISTINCT:
            lines.append(
                "      # Checked after the model's SELECT DISTINCT: the"
            )
            lines.append("      # source repeats byte-identical rows per NPI.")
        lines.append("      - dbt_utils.unique_combination_of_columns:")
        lines.append(f"          combination_of_columns: [{', '.join(key)}]")
        lines.append("      - not_null_proportion_multiple_columns:")
        lines.append(f"          at_least: {AT_LEAST}")
        if sparse:
            lines.append("          # Legitimately sparse: these fields are")
            lines.append("          # optional in the NPI application.")
            lines.append("          ignore_values:")
            for name in sparse:
                lines.append(f"            - {name}")
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
            lines.append(f"      - name: {c['name']}")
            lines.append("        description: " + block(c["description"], 10))
            if c["name"] in tmeta["not_null"]:
                lines.append("        tests: [not_null]")
        lines.append("")

    OUT.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    print(f"{OUT}: {len(meta)} models")


if __name__ == "__main__":
    main()
