"""Generate the dbt models and schema.yml for us_fec_campaign_finance.

    python gen_dbt.py

Both are derived from architecture/*.csv, which is the source of truth for column
names, order and types (.claude/rules/onboarding-workflow.md). Regenerate after any
architecture change rather than hand-editing the SQL.
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
MODELS = HERE.parent
DATASET = "us_fec_campaign_finance"

PARTITION_START = 1980
PARTITION_END = (
    2031  # last cycle (2026) + 5, per .claude/rules/bigquery-conventions.md
)

CAST = {
    "STRING": "safe_cast({c} as string) {c}",
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "DATE": "safe_cast({c} as date) {c}",
}

# Filers mistype transaction dates, and the FEC publishes what was filed: the raw
# data carries dates from 1899 to 2202. There are only ~128 such rows across 79M,
# but they are not harmless — the BD Pro rolling window is computed as
# max(transaction_date) - free_lag, so a single row dated 2202 puts free_end 176
# years in the future and silently drops the entire table into the free tier.
#
# Dates outside a plausible window are therefore set to NULL, and only the date is
# dropped — the row and all its other fields are kept. The bounds are deterministic
# rather than relative to the run date, so the model is reproducible: no federal
# filing predates the FEC's creation in 1975, and nothing can legitimately be dated
# more than a year past the end of its own two-year cycle.
BOUNDED_DATE = (
    "case\n"
    "        when safe_cast({c} as date)\n"
    "            between date(1975, 1, 1)\n"
    "            and date(safe_cast(year as int64), 12, 31)\n"
    "        then safe_cast({c} as date)\n"
    "    end {c}"
)

# Tables whose transaction_date feeds the BD Pro window.
DATE_BOUNDED_TABLES = {
    "contribution_individual",
    "contribution_committee",
    "committee_transaction",
    "disbursement",
}

# Tables partitioned by cycle; dicionario has no temporal dimension.
UNPARTITIONED = {"dicionario"}

TABLE_DESCRIPTION = {
    "candidate": (
        "Candidatos a cargos federais dos Estados Unidos registrados na Federal "
        "Election Commission, uma linha por candidato por ciclo eleitoral de dois "
        "anos. Inclui cargo disputado, estado e distrito, partido declarado, "
        "situação de incumbência e o comitê de campanha principal."
    ),
    "committee": (
        "Comitês registrados na Federal Election Commission, uma linha por comitê "
        "por ciclo eleitoral de dois anos. Abrange comitês de campanha de "
        "candidatos à Câmara, ao Senado e à Presidência, comitês partidários, "
        "PACs, Super PACs e PACs híbridos, com tesoureiro, tipo, designação e "
        "organização conectada."
    ),
    "candidate_committee_link": (
        "Ligações entre candidatos e os comitês que os apoiam, uma linha por par "
        "candidato-comitê por ciclo eleitoral. É a tabela que conecta candidate a "
        "committee e, por meio de committee_id, às tabelas de transações."
    ),
    "contribution_individual": (
        "Contribuições de pessoas físicas a comitês federais, uma linha por "
        "itemização declarada à Federal Election Commission. Inclui nome, cidade, "
        "estado, empregador e ocupação do contribuinte, além de valor, data e tipo "
        "de transação. É o arquivo de maior volume da FEC."
    ),
    "contribution_committee": (
        "Contribuições de comitês a candidatos e despesas independentes, uma linha "
        "por itemização declarada à Federal Election Commission. Cobre doações de "
        "PACs e comitês partidários a campanhas, gastos independentes a favor e "
        "contra candidatos e despesas coordenadas de partido."
    ),
    "committee_transaction": (
        "Transações entre comitês declaradas à Federal Election Commission, uma "
        "linha por itemização. Inclui transferências entre comitês filiados, "
        "recebimentos de comitês não registrados, empréstimos e reembolsos, "
        "completando o fluxo de recursos entre PACs, partidos e campanhas."
    ),
    "disbursement": (
        "Despesas operacionais de comitês federais informadas no Schedule B dos "
        "formulários da Federal Election Commission, uma linha por desembolso. "
        "Inclui beneficiário do pagamento, valor, data, finalidade declarada e "
        "categoria de despesa."
    ),
    "dicionario": (
        "Dicionário de códigos do conjunto us_fec_campaign_finance. Uma linha por "
        "combinação de tabela, coluna e código, com a descrição publicada pela "
        "Federal Election Commission."
    ),
}


def read_arch(table: str) -> list[dict]:
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def render_sql(table: str, arch: list[dict]) -> str:
    config = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table not in UNPARTITIONED:
        config.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {PARTITION_START}, '
            f'"end": {PARTITION_END}, "interval": 1}},\n'
            "        },"
        )

    def cast_for(a: dict) -> str:
        if a["name"] == "transaction_date" and table in DATE_BOUNDED_TABLES:
            return BOUNDED_DATE.format(c=a["name"])
        return CAST[a["bigquery_type"]].format(c=a["name"])

    casts = ",\n".join("    " + cast_for(a) for a in arch)
    return (
        "{{\n    config(\n"
        + "\n".join(config)
        + "\n    )\n}}\n\n\nselect\n"
        + casts
        + "\nfrom\n"
        + f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        + "    as t\n"
    )


def yaml_block(text: str, indent: int) -> str:
    """Render a description as a `>` block scalar (required by dbt-conventions)."""
    pad = " " * indent
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 74:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    return ">\n" + "\n".join(pad + ln for ln in lines)


# Column-level dbt tests, beyond the model-level ones every table gets.
NOT_NULL = {
    "candidate": ["year", "candidate_id"],
    "committee": ["year", "committee_id"],
    "candidate_committee_link": ["year", "candidate_id", "committee_id"],
    "contribution_individual": ["year", "committee_id", "sub_id"],
    "contribution_committee": ["year", "committee_id", "sub_id"],
    "committee_transaction": ["year", "committee_id", "sub_id"],
    "disbursement": ["year", "committee_id", "sub_id"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

UNIQUE_KEY = {
    "candidate": ["year", "candidate_id"],
    "committee": ["year", "committee_id"],
    "candidate_committee_link": ["year", "linkage_id"],
    "contribution_individual": ["year", "sub_id"],
    "contribution_committee": ["year", "sub_id"],
    "committee_transaction": ["year", "sub_id"],
    "disbursement": ["year", "sub_id"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

# Columns that are legitimately sparse, so the 5% not-null floor does not apply.
IGNORE_SPARSE = {
    "candidate": ["address_2", "office_district"],
    "committee": [
        "address_2",
        "candidate_id",
        "organization_type",
        "connected_organization_name",
        "treasurer_name",
    ],
    "candidate_committee_link": [],
    "contribution_individual": [
        "other_id",
        "memo_code",
        "memo_text",
        "election_type_year",
        "contributor_employer",
        "contributor_occupation",
    ],
    "contribution_committee": [
        "other_id",
        "memo_code",
        "memo_text",
        "contributor_employer",
        "contributor_occupation",
        "candidate_id",
    ],
    "committee_transaction": [
        "other_id",
        "memo_code",
        "memo_text",
        "election_type_year",
        "counterparty_employer",
        "counterparty_occupation",
    ],
    "disbursement": [
        "back_reference_transaction_id",
        "election_type_year",
        "category",
        "category_description",
        "memo_code",
        "memo_text",
        "purpose",
    ],
    "dicionario": ["cobertura_temporal"],
}

# office_state carries US on presidential candidacies, which the state directory
# does not (and should not) contain.
STATE_IGNORE = ["US"]

# Columns kept out of custom_dictionary_coverage because the FEC does not validate
# them on filing: the code list is a convention filers mostly follow, not a closed
# set the source enforces. Measured unmapped share (audit_codes.py):
#
#   candidate.party            0.28% of rows, 136 distinct — "GOP", "Rep", "Dem",
#   committee.party            0.70% of rows, 137 distinct — state abbreviations, typos
#   disbursement.category      0.90% of rows, 199 distinct — state abbreviations, digits
#
# They keep covered_by_dictionary=yes because the dicionario does explain >99% of
# values and is the right place for a reader to look. What would be wrong is gating
# CI on 100% closure of a field the source itself leaves open — the fix would be to
# invent dictionary entries for "0.6" and "---", which is worse than not testing.
# Every other coded column is a genuine closed set and is tested at 100%.
DICTIONARY_TEST_EXCLUDE = {
    ("candidate", "party"),
    ("committee", "party"),
    ("disbursement", "category"),
}


def render_schema() -> str:
    out = ["---", "version: 2", "models:"]
    for table in TABLE_DESCRIPTION:
        arch = read_arch(table)
        out.append(f"  - name: {DATASET}__{table}")
        out.append(
            f"    description: {yaml_block(TABLE_DESCRIPTION[table], 6)}"
        )
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            f"          combination_of_columns: [{', '.join(UNIQUE_KEY[table])}]"
        )
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if IGNORE_SPARSE[table]:
            out.append("          ignore_values:")
            out.extend(f"            - {c}" for c in IGNORE_SPARSE[table])
        coded = [
            a["name"]
            for a in arch
            if a["covered_by_dictionary"] == "yes"
            and (table, a["name"]) not in DICTIONARY_TEST_EXCLUDE
        ]
        if coded:
            out.append("      - custom_dictionary_coverage:")
            out.append("          columns_covered_by_dictionary:")
            out.extend(f"            - {c}" for c in coded)
            out.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
        out.append("    columns:")
        for a in arch:
            name = a["name"]
            out.append(f"      - name: {name}")
            out.append(
                f"        description: {yaml_block(a['description'], 10)}"
            )
            is_state_fk = (
                a["directory_column"]
                == "br_bd_diretorios_us.state:abbreviation"
            )
            simple = ["not_null"] if name in NOT_NULL[table] else []
            if not simple and not is_state_fk:
                continue
            out.append("        tests:")
            out.extend(f"          - {t}" for t in simple)
            if is_state_fk:
                out.append("          - custom_relationships:")
                out.append(
                    "              to: ref('br_bd_diretorios_us__state')"
                )
                out.append("              field: abbreviation")
                out.append(f"              ignore_values: {STATE_IGNORE}")
    return "\n".join(out) + "\n"


def main():
    for table in TABLE_DESCRIPTION:
        arch = read_arch(table)
        path = MODELS / f"{DATASET}__{table}.sql"
        path.write_text(render_sql(table, arch), encoding="utf-8")
        print(f"{path.name:52s} {len(arch):3d} columns")
    (MODELS / "schema.yml").write_text(render_schema(), encoding="utf-8")
    print("schema.yml")


if __name__ == "__main__":
    main()
