"""Generate the dbt models and schema.yml for us_stanford_dime.

Both are derived from ``architecture.py`` so the column set, order and types
cannot drift between the architecture, the staging Parquet and the built table.

Test scoping matters here. ``not_null_proportion_multiple_columns`` compiles a
scan over *every* column, and ``unique_combination_of_columns`` is a full
shuffle; run unscoped on an 861M-row table they burn a large slice of the
project's daily BigQuery quota. Both are therefore pinned to a single cycle on
the contribution table. DIME v4.0 is a static release, so a literal cycle is
honest — there is no pipeline that would advance it.

Sparse-column exemptions are read from ``sparsity.json``, which
``measure_sparsity.py`` computes from the **staging** table. Deriving them from
the built table instead would let a column destroyed by a bad cast look
legitimately empty and be excused by the very test meant to catch it.

    python gen_dbt.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch

CODE_DIR = Path(__file__).resolve().parent
MODEL_DIR = CODE_DIR.parent
DATASET = "us_stanford_dime"
SPARSITY_FILE = CODE_DIR / "sparsity.json"

# Cycles the expensive contribution tests are scoped to. Both are pinned rather
# than following the data, which is honest here because DIME v4.0 is a static
# release with no pipeline to advance them.
#
# They differ because the two tests have very different cost profiles.
# not_null_proportion_multiple_columns sums NULLs across all 45 columns in one
# pass, so it is scoped to the smallest cycle in which every column is
# meaningfully populated: efec_memo is 36.6% non-null in 2014 but only 0.9% in
# 2008, so an older scope would exempt a column that is genuinely there.
# unique_combination_of_columns touches two columns, so it can afford the
# newest and largest partition, which is where a key collision is likeliest.
SPARSITY_CYCLE = 2014
UNIQUENESS_CYCLE = 2024

PARTITIONED = {
    "contribution": (1980, 2030),
    "recipient": (1980, 2030),
    "contributor_cycle": (1980, 2030),
}

UNIQUE_KEYS = {
    # icpsr_id is the codebook's stated row identifier and is verified unique.
    "recipient": ["icpsr_id"],
    "contribution": ["cycle", "transaction_id"],
    "contributor": ["contributor_id"],
    "contributor_cycle": ["cycle", "contributor_id"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

# Only columns the data actually guarantees. recipient_id and contributor_id are
# deliberately absent: the source carries a handful of records with no
# identifier — 35 contributions across cycles 2006, 2012 and 2016, and 52
# contributor rows in which every other field is also empty. Those rows are kept
# rather than filtered, because dropping source records to make a test pass
# hides the defect instead of reporting it.
NOT_NULL = {
    "contribution": ["cycle", "transaction_id", "contributor_id"],
    "recipient": ["cycle", "recipient_id", "icpsr_id"],
    "contributor": [],
    "contributor_cycle": ["cycle", "amount"],
    "dicionario": ["id_tabela", "nome_coluna", "chave", "valor"],
}

# Tables whose key is unique among real records but where the identifier-less
# rows collapse into a single null group. Verified: contributor has 44,206,014
# distinct ids across 44,206,066 rows, exactly the row count minus its 52 null
# rows, so the non-null ids are perfectly unique.
UNIQUE_ALLOWANCE = {
    "contributor": 0.00001,
    "contributor_cycle": 0.00001,
}

# Directory foreign keys with a documented sentinel to skip. DIME uses 9999 for
# an unknown first active cycle on 666,508 contributor rows; last_cycle_active
# carries no sentinel and needs no exemption.
DIRECTORY_IGNORE = {
    ("contributor", "first_cycle_active"): ["9999"],
}

TABLE_DESCRIPTION = {
    "contribution": (
        "Itemized records of political contributions to federal, state and local "
        "elections in the United States, from the 1980 to the 2024 election cycle. "
        "Each row is one transaction between a donor and a recipient, carrying the "
        "DIME contributor and recipient identifiers, the geocoded donor location, "
        "and the common-space CFscores of both parties. Records sourced from "
        "CRP/NIMSP are excluded from the public release and so are absent here."
    ),
    "recipient": (
        "Candidates and committees that received political contributions, one row "
        "per recipient per election cycle, from 1980 to 2024. Carries fundraising "
        "totals, election outcomes, candidate characteristics and the full set of "
        "DIME ideology measures: CFscores, DW-DIME, the composite score and "
        "DW-NOMINATE. Includes recipients that did not meet the requirements for "
        "the CFscore scaling, flagged by included_in_scaling."
    ),
    "contributor": (
        "Individuals and organizations that made political contributions between "
        "1979 and 2024, one row per donor, resolved to a stable identifier across "
        "election cycles and levels of government. Carries the donor's CFscore, "
        "the attributes reported on their most recent contribution, and the window "
        "of cycles over which they were active."
    ),
    "contributor_cycle": (
        "Total amount contributed by each donor in each election cycle, from 1980 "
        "to 2024. Reshaped from the wide amount-per-cycle columns of the DIME "
        "contributor file; a donor-cycle pair with no giving is absent rather than "
        "stored as a zero."
    ),
    "dicionario": (
        "Dictionary mapping the codes stored in the coded columns of the "
        "us_stanford_dime tables to their labels."
    ),
}


def load_sparsity() -> dict:
    if SPARSITY_FILE.exists():
        return json.loads(SPARSITY_FILE.read_text())
    return {}


def model_sql(table: str) -> str:
    """Render one dbt model."""
    cols = arch.TABLES[table]
    config = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table in PARTITIONED:
        start, end = PARTITIONED[table]
        config += [
            "        partition_by={",
            '            "field": "cycle",',
            '            "data_type": "int64",',
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},',
            "        },",
        ]
    casts = []
    for name, bq_type, *_ in cols:
        t = bq_type.lower()
        if bq_type == "DATE":
            # safe_cast(<value> as date) accepts only a bare YYYY-MM-DD; routing
            # through datetime also accepts a timestamp spelling, so a source
            # that grows a time component does not silently empty the column.
            casts.append(f"    date(safe_cast({name} as datetime)) {name},")
        else:
            casts.append(f"    safe_cast({name} as {t}) {name},")
    casts[-1] = casts[-1].rstrip(",")
    return (
        "{{\n    config(\n"
        + "\n".join(config)
        + "\n    )\n}}\n\n\nselect\n"
        + "\n".join(casts)
        + "\nfrom "
        + '{{ set_datalake_project("'
        + f"{DATASET}_staging.{table}"
        + '") }} as t\n'
    )


def _yaml_block(text: str, indent: int) -> str:
    """Render a description as a folded block scalar.

    Always a block scalar: a bare scalar breaks YAML parsing the moment a
    description contains a colon.
    """
    pad = " " * indent
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 74:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    if cur:
        lines.append(cur)
    return ">-\n" + "\n".join(f"{pad}{ln}" for ln in lines)


def schema_yml() -> str:
    sparsity = load_sparsity()
    out = ["---", "version: 2", "models:"]
    for table in arch.TABLES:
        cols = arch.TABLES[table]
        scoped = table == "contribution"
        uniq_where = f'"cycle = {UNIQUENESS_CYCLE}"'
        sparse_where = f'"cycle = {SPARSITY_CYCLE}"'
        out.append(f"  - name: {DATASET}__{table}")
        out.append(
            f"    description: {_yaml_block(TABLE_DESCRIPTION[table], 6)}"
        )
        out.append("    tests:")
        allowance = UNIQUE_ALLOWANCE.get(table)
        kind = (
            "custom_unique_combinations_of_columns"
            if allowance
            else "dbt_utils.unique_combination_of_columns"
        )
        out.append(f"      - {kind}:")
        out.append(
            f"          combination_of_columns: [{', '.join(UNIQUE_KEYS[table])}]"
        )
        if allowance:
            # Render as a plain decimal: YAML 1.1 parses 1e-05 as a string.
            out.append(
                f"          proportion_allowed_failures: {allowance:.8f}"
            )
        if scoped:
            out.append("          config:")
            out.append(f"            where: {uniq_where}")
        ignore = sorted(sparsity.get(table, {}).get("sparse", []))
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if ignore:
            out.append("          ignore_values:")
            for c in ignore:
                out.append(f"            - {c}")
        if scoped:
            out.append("          config:")
            out.append(f"            where: {sparse_where}")
        out.append("    columns:")
        for col in cols:
            (
                name,
                _bq,
                desc,
                _tc,
                _dict,
                directory,
                _unit,
                _sens,
                obs,
                _orig,
            ) = col
            full = desc if not obs else f"{desc}. {obs}"
            out.append(f"      - name: {name}")
            out.append(f"        description: {_yaml_block(full, 10)}")
            tests = []
            if name in NOT_NULL[table]:
                tests.append("not_null")
            if tests or directory:
                out.append("        tests:")
                for t in tests:
                    out.append(f"          - {t}")
                if directory:
                    ref, field = directory.split(":")
                    ds, tbl = ref.split(".")
                    ignore = DIRECTORY_IGNORE.get((table, name))
                    kind = (
                        "custom_relationships" if ignore else "relationships"
                    )
                    out.append(f"          - {kind}:")
                    out.append(f"              to: ref('{ds}__{tbl}')")
                    # The time directory stores ano as a STRUCT, so the test has
                    # to bind to the inner field rather than the column.
                    inner = (
                        f"{field}.{field}"
                        if ds.endswith("data_tempo")
                        else field
                    )
                    out.append(f"              field: {inner}")
                    if ignore:
                        vals = ", ".join(f"'{v}'" for v in ignore)
                        out.append(f"              ignore_values: [{vals}]")
                    if scoped:
                        out.append("              config:")
                        out.append(f"                where: {sparse_where}")
    return "\n".join(out) + "\n"


def main() -> None:
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    for table in arch.TABLES:
        path = MODEL_DIR / f"{DATASET}__{table}.sql"
        path.write_text(model_sql(table))
        print(f"wrote {path.name}")
    (MODEL_DIR / "schema.yml").write_text(schema_yml())
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
