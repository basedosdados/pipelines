"""Generate the us_treasury_usaspending dbt models and schema.yml.

The transaction tables carry 297 and 112 columns, so the SQL is generated from
the architecture CSVs rather than hand-written — the architecture stays the
single source of truth for column order and type.

`schema.yml` sparse-column exemptions are not guessed: run
``null_proportions.py`` against the built dev tables and pass the result in via
``--sparse <json>`` so ``not_null_proportion_multiple_columns`` ignores exactly
the columns the source leaves mostly empty.

Usage:
    uv run python models/us_treasury_usaspending/code/build_dbt.py
    uv run python models/us_treasury_usaspending/code/build_dbt.py --sparse sparse.json
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
MODELS = HERE.parent
DATASET = "us_treasury_usaspending"
PARTITION = "fiscal_year"
FIRST_FY, LAST_FY = 2007, 2026

# Columns BigQuery clusters on, most selective prefix first. Chosen for the
# queries these tables exist to answer: who awarded it, to whom, and where.
CLUSTER = {
    "contract_transaction": [
        "awarding_agency_code",
        "recipient_uei",
        "recipient_state_code",
    ],
    "assistance_transaction": [
        "awarding_agency_code",
        "cfda_number",
        "recipient_state_code",
    ],
}

# The source ships a subset of county FIPS values float-corrupted: the county
# part loses its leading zero and a ".0" is appended, so Ohio/Franklin (39049)
# arrives as "3949.0" and Texas/Travis as "48453.0". Stripping the suffix and
# re-padding the county part to three digits recovers the real code and is
# idempotent for the values that are already correct.
COUNTY_FIPS_COLUMNS = {
    "prime_award_transaction_recipient_county_fips_code",
    "prime_award_transaction_place_of_performance_county_fips_code",
}


def county_fips_expr(col: str) -> str:
    stripped = f"regexp_replace({col}, r'\\.0$', '')"
    return f"concat(substr({stripped}, 1, 2), lpad(substr({stripped}, 3), 3, '0'))"


UNIQUE_KEY = {
    "contract_transaction": "contract_transaction_id",
    "assistance_transaction": "assistance_transaction_id",
}

# Renames the cleaning step already applied, so these columns arrive in staging
# under the new name. Everything else is read from staging under its source
# spelling and renamed here.
STAGING_RENAMES = {
    "action_date_fiscal_year": "fiscal_year",
    "outlayed_amount_from_COVID-19_supplementals_for_overall_award": "outlayed_amount_from_covid19_supplementals_for_overall_award",
    "obligated_amount_from_COVID-19_supplementals_for_overall_award": "obligated_amount_from_covid19_supplementals_for_overall_award",
    "1862_land_grant_college": "land_grant_college_1862",
    "1890_land_grant_college": "land_grant_college_1890",
    "1994_land_grant_college": "land_grant_college_1994",
}


def staging_column(row: dict) -> str:
    """Name the column has in the staging table."""
    original = row["original_name"]
    return STAGING_RENAMES.get(original, original)


TABLE_DESCRIPTION = {
    "contract_transaction": (
        "Transações de contratos federais norte-americanos registradas no FPDS e "
        "publicadas pelo USAspending.gov, uma linha por ação contratual (assinatura "
        "ou aditivo), com agência concedente e financiadora, contratado, valores "
        "empenhados, objeto, local de execução e características da contratação"
    ),
    "assistance_transaction": (
        "Transações de assistência financeira federal norte-americana — subvenções, "
        "empréstimos, pagamentos diretos e seguros — publicadas pelo USAspending.gov, "
        "uma linha por ação, com agência concedente e financiadora, beneficiário, "
        "programa CFDA, valores empenhados e local de execução"
    ),
    "dicionario": (
        "Dicionário dos valores codificados das tabelas do conjunto, com uma linha "
        "por par de chave e valor, construído a partir dos domínios do dicionário de "
        "elementos do DATA Act"
    ),
}


def read_arch(table: str) -> list[dict]:
    with (ARCH / f"{table}.csv").open() as f:
        return list(csv.DictReader(f))


def sql_for(table: str, rows: list[dict]) -> str:
    casts = []
    for r in rows:
        name, btype = r["name"], r["bigquery_type"]
        source = staging_column(r)
        expr = (
            county_fips_expr(source) if name in COUNTY_FIPS_COLUMNS else source
        )
        casts.append(f"    safe_cast({expr} as {btype.lower()}) {name},")
    casts[-1] = casts[-1].rstrip(",")
    body = "\n".join(casts)

    if table == "dicionario":
        config = (
            "{{\n"
            "    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
            "    )\n"
            "}}"
        )
    else:
        cluster = ", ".join(f'"{c}"' for c in CLUSTER[table])
        config = (
            "{{\n"
            "    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            f'            "field": "{PARTITION}",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {FIRST_FY}, "end": {LAST_FY + 5}, "interval": 1}},\n'
            "        },\n"
            f"        cluster_by=[{cluster}],\n"
            "    )\n"
            "}}"
        )

    return (
        f"{config}\n\n\n"
        "select\n"
        f"{body}\n"
        "from\n"
        f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        "    as t\n"
    )


def yaml_block(text: str, indent: int) -> str:
    pad = " " * indent
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 76:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    if cur:
        lines.append(cur)
    return "\n".join(pad + ln for ln in lines)


def schema_yaml(
    arch: dict[str, list[dict]],
    sparse: dict[str, list[str]],
    covered: dict[str, dict],
) -> str:
    out = ["---", "version: 2", "models:"]
    for table, rows in arch.items():
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >")
        out.append(yaml_block(TABLE_DESCRIPTION[table], 6))
        out.append("    tests:")
        if table == "dicionario":
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(
                "          combination_of_columns: [id_tabela, nome_coluna, chave]"
            )
            out.append("      - not_null_proportion_multiple_columns:")
            out.append("          at_least: 0.05")
            out.append("          ignore_values:")
            out.append("            - cobertura_temporal")
        else:
            key = UNIQUE_KEY[table]
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(
                f"          combination_of_columns: [{PARTITION}, {key}]"
            )
            out.append("          config:")
            out.append("            where: __most_recent_fiscal_year__")
            # Deliberately unscoped: the shared macro discovers this test's
            # columns by introspecting the where-subquery, and that returns the
            # staging column names, which differ here (*_unique_key -> *_id).
            # A full-history pass is a single aggregate and also the stricter
            # reading of "mostly empty".
            out.append("      - not_null_proportion_multiple_columns:")
            out.append("          at_least: 0.05")
            cols = sparse.get(table, [])
            if cols:
                out.append("          ignore_values:")
                out.extend(f"            - {c}" for c in cols)
            # Only the columns measured as fully covered carry the dictionary
            # test. The rest are still coded — and still flagged
            # covered_by_dictionary — but the DATA Act published domains do not
            # enumerate every value the data holds: business_types_code stores
            # concatenations of single-letter codes, and a few code sets have
            # grown sentinels the domain never listed.
            dict_cols = (covered.get(table) or {}).get("covered", [])
            if dict_cols:
                out.append("      - custom_dictionary_coverage:")
                out.append("          columns_covered_by_dictionary:")
                out.extend(f"            - {c}" for c in dict_cols)
                out.append(
                    f"          dictionary_model: ref('{DATASET}__dicionario')"
                )
        out.append("    columns:")
        for r in rows:
            out.append(f"      - name: {r['name']}")
            out.append("        description: >")
            out.append(yaml_block(r["description"], 10))
            tests = []
            if r["name"] in (PARTITION, UNIQUE_KEY.get(table)):
                tests.append("        tests: [not_null]")
            # County keeps its directory_column link in the backend metadata,
            # but carries no relationships test: state-wide aggregate records
            # use an "<state>000" sentinel that is not a county, and retired
            # codes (pre-2022 Connecticut, Dade before Miami-Dade, Ormsby NV)
            # legitimately do not resolve against a current-vintage directory.
            if r["directory_column"] and r["name"] not in COUNTY_FIPS_COLUMNS:
                ref_table, field = (
                    r["directory_column"].split(".")[1].split(":")
                )
                tests.append("        tests:")
                tests.append("          - relationships:")
                tests.append(
                    f"              to: ref('br_bd_diretorios_us__{ref_table}')"
                )
                tests.append(f"              field: {field}")
                tests.append("              config:")
                tests.append(
                    "                where: __most_recent_fiscal_year__"
                )
            out.extend(tests)
    return "\n".join(out) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--sparse",
        help="JSON file mapping table -> columns to exempt from the null-proportion test",
    )
    ap.add_argument(
        "--covered",
        help="JSON file from dictionary_coverage.py naming the columns the dicionario fully covers",
    )
    args = ap.parse_args()
    sparse = json.loads(Path(args.sparse).read_text()) if args.sparse else {}
    covered = (
        json.loads(Path(args.covered).read_text()) if args.covered else {}
    )

    arch = {
        t: read_arch(t)
        for t in (
            "contract_transaction",
            "assistance_transaction",
            "dicionario",
        )
    }
    for table, rows in arch.items():
        path = MODELS / f"{DATASET}__{table}.sql"
        path.write_text(sql_for(table, rows))
        print(f"{path.name}: {len(rows)} columns")

    (MODELS / "schema.yml").write_text(schema_yaml(arch, sparse, covered))
    print(
        f"schema.yml: {sum(len(r) for r in arch.values())} columns documented"
    )


if __name__ == "__main__":
    main()
