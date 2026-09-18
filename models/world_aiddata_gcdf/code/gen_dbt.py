"""Generate the dbt model (.sql) and schema.yml for world_aiddata_gcdf.

DRY with the architecture: column names, order, types, and Portuguese
descriptions all come from ``architecture/gen_architecture.py``. Run after
editing the architecture spec to regenerate both dbt artifacts.

Outputs (into the model directory, models/world_aiddata_gcdf/):
    world_aiddata_gcdf__projects.sql
    schema.yml
"""

import importlib.util
from pathlib import Path

_HERE = Path(__file__).resolve().parent
MODEL_DIR = _HERE.parent
_spec = importlib.util.spec_from_file_location(
    "gcdf_arch", _HERE / "architecture" / "gen_architecture.py"
)
arch = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(arch)
COLS = arch.COLS
OUTPUT_ORDER = arch.OUTPUT_ORDER
BYNAME = {c["t"]: c for c in COLS}

DATASET = "world_aiddata_gcdf"
TABLE = "projects"

CAST = {
    "STRING": "string",
    "INT64": "int64",
    "FLOAT64": "float64",
    "DATE": "date",
}

# Columns legitimately <5% populated (computed from the cleaned data); excluded
# from the not_null_proportion test. Keep in sync with clean.py output.
SPARSE = [
    "indirect_receiving_agencies",
    "indirect_receiving_agencies_type",
    "on_lending",
    "guarantor",
    "guarantor_agency_type",
    "insurance_provider",
    "insurance_provider_agency_type",
    "collateral_provider",
    "collateral_provider_agency_type",
    "security_agent",
    "security_agent_type",
    "collateral",
    "financial_distress",
    "actual_implementation_start_date_estimated",
    "deviation_planned_implementation_start_date",
    "actual_completion_date_estimated",
    "deviation_planned_completion_date",
    "management_fee",
    "commitment_fee",
    "insurance_fee_percent",
    "insurance_fee_nominal_usd",
    "default_interest_rate",
    "suppliers_credit",
    "interest_free_loan",
    "refinancing",
    "mergers_acquisitions",
    "working_capital",
    "epcf",
    "lease",
    "fxsl_bop",
    "cc_irs",
    "rcf",
    "gcl",
    "pbc",
    "pxf_commodity_prepayment",
    "inter_bank_loan",
    "overseas_project_contracting_loan",
    "dpa",
    "project_finance",
    "involving_multilateral",
    "short_term",
    "rescue",
    "jv_spv_host_government_ownership",
    "jv_spv_chinese_government_ownership",
]

TABLE_DESC_PT = (
    "Registros a nível de projeto/atividade do financiamento oficial chinês para o "
    "desenvolvimento (empréstimos e doações) destinado a países de baixa e média renda "
    "entre 2000 e 2021, compilados pela AidData na versão 3.0 do Global Chinese "
    "Development Finance Dataset segundo a metodologia TUFF 3.0"
)


def gen_sql() -> str:
    lines = [
        "{{",
        "    config(",
        f'        schema="{DATASET}",',
        f'        alias="{TABLE}",',
        '        materialized="table",',
        "        partition_by={",
        '            "field": "year",',
        '            "data_type": "int64",',
        '            "range": {"start": 2000, "end": 2026, "interval": 1},',
        "        },",
        "    )",
        "}}",
        "",
        "",
        "select",
    ]
    body = []
    for name in OUTPUT_ORDER:
        ty = CAST[BYNAME[name]["ty"]]
        body.append(f"    safe_cast({name} as {ty}) {name},")
    body[-1] = body[-1].rstrip(",")  # last column: no trailing comma
    lines += body
    lines.append("from")
    lines.append(
        f'    {{{{ set_datalake_project("{DATASET}_staging.{TABLE}") }}}}'
    )
    lines.append("    as t")
    lines.append("")
    return "\n".join(lines)


def _yaml_desc(text: str, indent: int) -> str:
    pad = " " * indent
    return f"{pad}description: >\n{pad}  {text}"


def gen_schema() -> str:
    out = ["---", "version: 2", "models:", f"  - name: {DATASET}__{TABLE}"]
    out.append(_yaml_desc(TABLE_DESC_PT, 4))
    out.append("    tests:")
    out.append("      - dbt_utils.unique_combination_of_columns:")
    out.append("          combination_of_columns: [id_record]")
    out.append("      - not_null_proportion_multiple_columns:")
    out.append("          at_least: 0.05")
    out.append("          ignore_values:")
    for s in SPARSE:
        out.append(f"            - {s}")
    out.append("    columns:")
    for name in OUTPUT_ORDER:
        c = BYNAME[name]
        _en, pt, _es = c["desc"]
        out.append(f"      - name: {name}")
        out.append(_yaml_desc(pt, 8))
        tests = []
        if name == "year":
            tests.append(("not_null", None))
            tests.append(
                (
                    "relationships",
                    ("br_bd_diretorios_data_tempo__ano", "ano.ano", None),
                )
            )
        elif name == "id_record":
            tests.append(("not_null", None))
        elif name == "country_iso3_code":
            tests.append(
                (
                    "custom_relationships",
                    ("br_bd_diretorios_mundo__pais", "sigla_iso3", []),
                )
            )
        if tests:
            out.append("        tests:")
            for tname, arg in tests:
                if tname == "not_null":
                    out.append("          - not_null")
                elif tname == "relationships":
                    ref, field, _ = arg
                    out.append("          - relationships:")
                    out.append(f"              to: ref('{ref}')")
                    out.append(f"              field: {field}")
                elif tname == "custom_relationships":
                    ref, field, ignore = arg
                    out.append("          - custom_relationships:")
                    out.append(f"              to: ref('{ref}')")
                    out.append(f"              field: {field}")
                    if ignore:
                        out.append(f"              ignore_values: {ignore}")
                    # Regional/multi-country records carry a NULL ISO-3 code; the
                    # macro counts NULLs as failures, so exclude them from the test.
                    out.append("              config:")
                    out.append(f'                where: "{name} is not null"')
    out.append("")
    return "\n".join(out)


if __name__ == "__main__":
    (MODEL_DIR / f"{DATASET}__{TABLE}.sql").write_text(gen_sql())
    (MODEL_DIR / "schema.yml").write_text(gen_schema())
    print(f"wrote {DATASET}__{TABLE}.sql and schema.yml to {MODEL_DIR}")
