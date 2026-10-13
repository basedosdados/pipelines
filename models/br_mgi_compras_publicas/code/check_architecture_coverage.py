"""Assert every field the API returns is mapped in the architecture, and vice versa.

Reads the sampled API schemas captured during design and compares them against the
`original_name` column of each architecture CSV. Catches fields silently dropped.
"""

import csv
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"

# Columns that are derived rather than copied from a source field.
DERIVED = {"ano", "data_extracao"}

# Source fields consumed by a derived column rather than mapped one-to-one.
CONSUMED = {
    "compra_sem_licitacao": {"dt_ano_aviso"},
    "compra_sem_licitacao_item": {"dt_ano_aviso_licitacao"},
}


def main(schemas_path: Path | str) -> int:
    schemas = json.loads(Path(schemas_path).read_text())
    problems = 0
    for table, spec in sorted(schemas.items()):
        path = ARCH / f"{table}.csv"
        if not path.exists():
            print(f"MISSING architecture CSV for {table}")
            problems += 1
            continue
        with path.open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        mapped = {r["original_name"] for r in rows if r["original_name"]}
        derived = {r["name"] for r in rows if not r["original_name"]}

        source = set(spec["fields"])
        unmapped = source - mapped - CONSUMED.get(table, set())
        unknown = mapped - source
        bad_derived = derived - DERIVED

        if unmapped:
            print(
                f"{table}: source fields not in architecture -> {sorted(unmapped)}"
            )
            problems += 1
        if unknown:
            print(
                f"{table}: architecture references unknown source fields -> {sorted(unknown)}"
            )
            problems += 1
        if bad_derived:
            print(
                f"{table}: columns with no original_name and not derived -> {sorted(bad_derived)}"
            )
            problems += 1
        if not (unmapped or unknown or bad_derived):
            print(
                f"ok  {table:<28} {len(source):>3} source fields, {len(rows):>3} columns"
            )
    return problems


if __name__ == "__main__":
    default = HERE / "api_field_samples.json"
    sys.exit(1 if main(sys.argv[1] if len(sys.argv) > 1 else default) else 0)
