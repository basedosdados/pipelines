"""Generate the dbt models and schema.yml for br_mgi_compras_publicas.

The architecture CSVs are the source of truth for column order, type and
description, so the SQL is generated rather than hand-written: nineteen tables
and 534 columns is well past the point where hand-editing stays consistent.

Usage:  uv run --no-project python build_dbt.py
"""

from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
MODELS = HERE.parent
REPO_ROOT = HERE.parents[2]
DATASET = "br_mgi_compras_publicas"

sys.path.insert(0, str(HERE))
from dbt_spec import TABLES  # noqa: E402

# Directory foreign keys, mapped to the ref() and field a dbt relationships test
# needs. The time directory is nested: binding to a bare `ano` resolves to the
# STRUCT and the test can never pass, so the field must be `ano.ano`.
DIRECTORY_TESTS = {
    "br_bd_diretorios_data_tempo.ano:ano": (
        "br_bd_diretorios_data_tempo__ano",
        "ano.ano",
    ),
    # The UF directory names its key column `sigla`. Binding to `sigla_uf`
    # fails with "Unrecognized name: sigla_uf; Did you mean sigla?".
    "br_bd_diretorios_brasil.uf:sigla": (
        "br_bd_diretorios_brasil__uf",
        "sigla",
    ),
    "br_bd_diretorios_brasil.municipio:id_municipio": (
        "br_bd_diretorios_brasil__municipio",
        "id_municipio",
    ),
}

# Row counts past which an unscoped test is not worth the bytes. A
# not_null_proportion test compiles to a scan of every column, so on a wide,
# multi-million-row table it alone can eat a meaningful slice of the daily
# BigQuery quota.
SCOPED_TEST_CONFIG = "        config:\n          where: __most_recent_year__\n"


# Phrases the architecture uses to record that a column is empty or nearly so
# at source. Deriving the null-proportion exclusions from these keeps the tests
# and the documented expectations from drifting apart.
EMPTY_MARKERS = (
    "Integralmente vazia",
    "Preenchido para menos de",
    "Vazio para",
)


def sparse_columns(columns: list[dict[str, str]]) -> list[str]:
    """Columns the architecture already documents as empty or nearly empty."""
    return [
        c["name"]
        for c in columns
        if any(marker in c["observations"] for marker in EMPTY_MARKERS)
    ]


def read_architecture(table: str) -> list[dict[str, str]]:
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def build_sql(table: str) -> str:
    spec = TABLES[table]
    columns = read_architecture(table)

    config = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if spec.partition == "ano":
        start, end = spec.year_range
        config.append(
            "        partition_by={\n"
            '            "field": "ano",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        },"
        )
    elif spec.partition == "data_extracao":
        config.append(
            "        partition_by={\n"
            '            "field": "data_extracao",\n'
            '            "data_type": "date",\n'
            '            "granularity": "day",\n'
            "        },"
        )

    casts = [
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) {c['name']},"
        for c in columns
    ]
    casts[-1] = casts[-1].rstrip(",")

    body = "\n".join(casts)
    sql = (
        "{{\n    config(\n"
        + "\n".join(config)
        + "\n    )\n}}\n\n\nselect\n"
        + body
        + "\nfrom\n"
        + f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        + "    as t\n"
    )
    if spec.dedup_order:
        # The API repeats records across pages, and sometimes records the same
        # logical row twice a second apart. Partition on the row's own content,
        # excluding those volatile timestamps, rather than on the key: keying the
        # partition is tidier but destroys data, because BigQuery groups every
        # NULL into a single partition and several of these keys are nullable --
        # on ata items that would have discarded 15,907 legitimate records.
        partition_columns = [
            c["name"] for c in columns if c["name"] not in spec.dedup_exclude
        ]
        joined = ",\n            ".join(partition_columns)
        sql += (
            "qualify\n"
            "    row_number() over (\n"
            f"        partition by\n            {joined}\n"
            f"        order by {spec.dedup_order} desc\n"
            "    )\n"
            "    = 1\n"
        )
    return sql


def _yaml_block(text: str, indent: str) -> str:
    """Emit a description as a folded block scalar on one line.

    Bare scalars break YAML the moment a description contains a colon, and these
    are Portuguese sentences that routinely do. The line is left unwrapped
    because the repo's yamlfix hook rewraps it; wrapping here as well would make
    the generator and the hook disagree and churn the file on every run.
    """
    return ">-\n" + indent + " ".join(text.split())


def build_schema_entry(table: str) -> str:
    spec = TABLES[table]
    columns = read_architecture(table)
    names = [c["name"] for c in columns]
    scoped = SCOPED_TEST_CONFIG if spec.scope_tests else ""

    out = [f"  - name: {DATASET}__{table}"]
    out.append("    description: " + _yaml_block(spec.description, "      "))
    out.append("    tests:")
    if spec.unique_tolerance:
        out.append("      - custom_unique_combinations_of_columns:")
        out.append(
            f"          combination_of_columns: [{', '.join(spec.key)}]"
        )
        out.append(
            f"          proportion_allowed_failures: {spec.unique_tolerance}"
        )
    else:
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            f"          combination_of_columns: [{', '.join(spec.key)}]"
        )
    if scoped:
        out.append(
            scoped.rstrip("\n")
            .replace("        config", "          config")
            .replace("          where", "            where")
        )
    out.append("      - not_null_proportion_multiple_columns:")
    out.append("          at_least: 0.05")
    ignore = sorted(set(sparse_columns(columns)) | set(spec.ignore_values))
    if ignore:
        out.append(f"          ignore_values: [{', '.join(ignore)}]")
    if scoped:
        out.append(
            scoped.rstrip("\n")
            .replace("        config", "          config")
            .replace("          where", "            where")
        )

    out.append("    columns:")
    for column in columns:
        name = column["name"]
        out.append(f"      - name: {name}")
        out.append(
            "        description: "
            + _yaml_block(column["description"], "          ")
        )
        tests: list[str] = []
        # A nullable key column cannot carry not_null. Where the source leaves
        # the key blank the tolerance is non-zero, and the relaxed uniqueness
        # test covers it instead.
        if name == spec.partition or (
            name in spec.key and not spec.unique_tolerance
        ):
            tests.append("not_null")
        directory = column["directory_column"]
        # Relationships tests scan the whole model; on the multi-million-row
        # item tables that cost is not worth re-proving a directory join that
        # the header table already covers.
        if directory in DIRECTORY_TESTS and not spec.scope_tests:
            ref, field = DIRECTORY_TESTS[directory]
            if tests:
                out.append("        tests:")
                out.append(f"          - {tests[0]}")
            else:
                out.append("        tests:")
            out.append("          - relationships:")
            out.append(f"              to: ref('{ref}')")
            out.append(f"              field: {field}")
            if name == "sigla_uf":
                # UASGs abroad -- embassies, consulates, military attaches --
                # carry the pseudo-unit EX, which the 27-entry UF directory
                # cannot hold. It is real source data, not a defect, so the
                # join is scoped rather than the rows dropped.
                out.append("              config:")
                out.append("                where: sigla_uf != 'EX'")
            continue
        if tests:
            out.append(f"        tests: [{', '.join(tests)}]")
    _ = names
    return "\n".join(out)


def format_in_place(paths: list[Path]) -> None:
    """Run the repo's own sqlfmt and yamlfix hooks over the generated files.

    Invoked through pre-commit rather than the bare binaries so the pinned hook
    versions and their arguments are used; formatting with a different sqlfmt
    build would leave the files churning on every commit.
    """
    names = [str(p) for p in paths]
    for hook in ("sqlfmt", "yamlfix"):
        result = subprocess.run(
            ["uv", "run", "pre-commit", "run", hook, "--files", *names],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
        # These hooks exit non-zero precisely when they reformatted something,
        # which is the expected outcome here.
        state = "reformatted" if result.returncode else "already canonical"
        print(f"  {hook}: {state}")


def main() -> None:
    written = []
    for table in TABLES:
        path = MODELS / f"{DATASET}__{table}.sql"
        path.write_text(build_sql(table), encoding="utf-8")
        written.append(path)
        print(f"  wrote {path.name}")

    schema = ["---", "version: 2", "models:"]
    schema.extend(build_schema_entry(table) for table in TABLES)
    schema_path = MODELS / "schema.yml"
    schema_path.write_text("\n".join(schema) + "\n", encoding="utf-8")
    written.append(schema_path)
    print(f"  wrote schema.yml ({len(TABLES)} models)")

    format_in_place(written)


if __name__ == "__main__":
    main()
