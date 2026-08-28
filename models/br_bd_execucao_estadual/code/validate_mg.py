"""Reconcile the harmonized MG `despesa` table against its raw staging fact table.

The harmonization is a chain of LEFT JOINs onto `mg_ft_despesa`. Two things can go wrong
and neither raises: a join can *fan out* (a duplicated dimension key multiplies rows and
inflates every total), or a `safe_cast` can quietly yield NULL for a whole column, leaving
it empty while dbt tests still pass. So the gate is row count and value totals against the
raw source, to the cent, plus per-column null rates so an emptied column is visible.

Everything is computed in ONE pass per table. These are large tables, and issuing a scalar
subquery per metric re-scans the whole thing each time -- a MiDES validation written that
way billed 63.87 GB.

Usage:
    uv run python models/br_bd_execucao_estadual/code/validate_mg.py [--env dev]
"""

from __future__ import annotations

import sys
import warnings

warnings.filterwarnings("ignore")

from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
ENV = "dev"
if "--env" in _argv:
    ENV = _argv[_argv.index("--env") + 1]

PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET = "br_bd_execucao_estadual"
STAGING = f"{PROJECT}.{DATASET}_staging"
PROD = f"{PROJECT}.{DATASET}"

# Raw totals straight off the staging mirror, in a single scan.
RAW = f"""
select
    count(*) as n_rows,
    count(distinct id_empenho) as n_empenhos,
    round(sum(safe_cast(vr_empenhado as float64)), 2) as vr_empenhado,
    round(sum(safe_cast(vr_liquidado as float64)), 2) as vr_liquidado,
    round(sum(safe_cast(vr_pago as float64)), 2) as vr_pago,
    -- staging is all-STRING, so `ano` must be cast before it can be compared with
    -- the model's INT64 column; otherwise '2002' != 2002 and the check fails on
    -- values that are in fact identical.
    min(safe_cast(ano as int64)) as ano_min,
    max(safe_cast(ano as int64)) as ano_max
from `{STAGING}.mg_ft_despesa`
"""

# The same totals off the harmonized table, plus join-coverage rates. Every `countif` here
# is a column that a broken join or a bad cast would silently empty.
MODEL = f"""
select
    count(*) as n_rows,
    count(distinct id_empenho) as n_empenhos,
    round(sum(valor_empenhado), 2) as vr_empenhado,
    round(sum(valor_liquidado), 2) as vr_liquidado,
    round(sum(valor_pago), 2) as vr_pago,
    min(ano) as ano_min,
    max(ano) as ano_max,
    countif(data is not null) / count(*) as cov_data,
    countif(numero_empenho is not null) / count(*) as cov_numero_empenho,
    countif(nome_credor is not null) / count(*) as cov_credor,
    countif(documento_credor is not null) / count(*) as cov_documento,
    countif(funcao is not null) / count(*) as cov_funcao,
    countif(subfuncao is not null) / count(*) as cov_subfuncao,
    countif(programa is not null) / count(*) as cov_programa,
    countif(acao is not null) / count(*) as cov_acao,
    countif(elemento_despesa is not null) / count(*) as cov_elemento,
    countif(fonte_recurso is not null) / count(*) as cov_fonte,
    countif(orgao is not null) / count(*) as cov_orgao,
    countif(id_unidade_gestora is not null) / count(*) as cov_unidade_gestora,
    countif(id_licitacao_bd is not null) / count(*) as cov_licitacao
from `{PROD}.despesa`
where sigla_uf = 'MG'
"""

# Coverage floors. A join that lands below these is a defect, not a data quirk. The
# licitação bridge is deliberately absent from this list: MG only links procurement to
# empenhos for the subset of spending that went through a purchase process, so a low rate
# there is expected and is reported rather than enforced.
MIN_COVERAGE = {
    "cov_data": 0.95,
    "cov_numero_empenho": 0.95,
    "cov_credor": 0.95,
    "cov_documento": 0.95,
    "cov_funcao": 0.95,
    "cov_subfuncao": 0.95,
    "cov_programa": 0.90,
    "cov_acao": 0.90,
    "cov_elemento": 0.95,
    "cov_fonte": 0.95,
    "cov_orgao": 0.90,
    "cov_unidade_gestora": 0.95,
}


# Every dimension joined by the despesa model, with the key it is joined on. A duplicate
# key multiplies fact rows through the LEFT JOIN and inflates every total, so this runs
# BEFORE the reconciliation -- it names the offending table, where a row-count mismatch
# alone would only tell you something is wrong.
#
# This is not hypothetical. `mg_dm_empenho` was first built with a `dm_empenho_desp_*`
# glob, which also matched `dm_empenho_desp_compras_empenho.csv.gz` -- the procurement
# subset of the same dimension, identical schema, so it merged silently and contributed
# 1,098,339 duplicate ids. Nothing raised.
DIMENSION_KEYS = [
    ("mg_dm_empenho", "id_empenho"),
    ("mg_dm_favorecido", "id_favorecido"),
    ("mg_dm_funcao", "id_funcao"),
    ("mg_dm_subfuncao", "id_subfuncao"),
    ("mg_dm_programa", "id_programa"),
    ("mg_dm_acao", "id_acao"),
    ("mg_dm_elemento", "id_elemento"),
    ("mg_dm_item", "id_item"),
    ("mg_dm_fonte", "id_fonte"),
    ("mg_dm_unidade_orc", "id_unidade_orc"),
    ("mg_dm_categoria", "id_categ_econ"),
    ("mg_dm_grupo", "id_grupo"),
    ("mg_dm_modalidade_aplic", "id_modalidade_aplic"),
    ("mg_dm_tipo_documento", "id_tipo_documento"),
    ("mg_dm_processo", "id_processo"),
    # joined by licitacao_item
    ("mg_dm_contratado", "id_contratado"),
    ("mg_dm_item_matserv", "id_item_matserv"),
    ("mg_dm_material_servico", "id_material_servico"),
    ("mg_dm_grupo_matserv", "id_grupo_matserv"),
    ("mg_dm_classe_matserv", "id_classe_matserv"),
    ("mg_dm_unidade_medida", "id_unidade_medida"),
]


def check_keys(client: bigquery.Client) -> list[str]:
    """Confirm every joined dimension key is unique. One query, one scan per table."""
    sql = "\nunion all\n".join(
        f"select '{table}' as tbl, '{key}' as key_col, count(*) as n, "
        f"count(distinct {key}) as n_distinct from `{STAGING}.{table}`"
        for table, key in DIMENSION_KEYS
    )
    failures = []
    print("=== dimension key uniqueness ===")
    print(f"{'table':26} {'key':22} {'rows':>12} {'distinct':>12}  status")
    for r in sorted(client.query(sql).result(), key=lambda r: r.tbl):
        ok = r.n == r.n_distinct
        if not ok:
            failures.append(
                f"{r.tbl}.{r.key_col}: {r.n:,} rows vs {r.n_distinct:,} distinct "
                f"-> fan-out risk"
            )
        print(
            f"{r.tbl:26} {r.key_col:22} {r.n:12,} {r.n_distinct:12,}  "
            f"{'unique' if ok else 'DUPLICATED'}"
        )
    return failures


def main() -> None:
    client = bigquery.Client(project=PROJECT)

    failures: list[str] = check_keys(client)
    if failures:
        # Stop here: with a duplicated key the totals below are guaranteed wrong, and
        # reporting them would only obscure the actual cause.
        print("\nFAILED:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)

    raw = next(iter(client.query(RAW).result()))
    mod = next(iter(client.query(MODEL).result()))
    print()

    print("=== MG: harmonized despesa vs raw ft_despesa ===")
    print(f"{'metric':22} {'raw':>22} {'model':>22}  status")

    # Counts must match EXACTLY. A LEFT JOIN cannot drop rows, so any difference is a
    # fan-out from a duplicated dimension key.
    for key in ("n_rows", "n_empenhos"):
        r, m = raw[key], mod[key]
        ok = r == m
        if not ok:
            failures.append(f"{key}: raw {r!r} != model {m!r}")
        print(f"{key:22} {r!s:>22} {m!s:>22}  {'OK' if ok else 'MISMATCH'}")

    # Money totals are compared on RELATIVE difference, not exactly. Summing 80M float64
    # values in a different order gives a different last cent -- float addition is not
    # associative, and BigQuery does not promise an order. On R$1.8 trillion the observed
    # gap is R$0.01, a relative 5e-15. Demanding equality here would fail forever on
    # arithmetic, not on data. The bar is tight enough that a genuinely lost or duplicated
    # row (the smallest of which moves the total far more) still trips it.
    for key in ("vr_empenhado", "vr_liquidado", "vr_pago"):
        r, m = raw[key], mod[key]
        rel = abs(r - m) / max(abs(r), 1.0)
        ok = rel < 1e-12
        if not ok:
            failures.append(
                f"{key}: raw {r!r} vs model {m!r} (relative {rel:.2e})"
            )
        print(
            f"{key:22} {r!s:>22} {m!s:>22}  "
            f"{'OK' if ok else 'MISMATCH'} (rel {rel:.1e})"
        )

    for key in ("ano_min", "ano_max"):
        r, m = raw[key], mod[key]
        if r != m:
            failures.append(f"{key}: raw {r} != model {m}")
        print(
            f"{key:22} {r!s:>22} {m!s:>22}  {'OK' if r == m else 'MISMATCH'}"
        )

    print("\n=== join coverage ===")
    for key, floor in MIN_COVERAGE.items():
        value = mod[key]
        ok = value >= floor
        if not ok:
            failures.append(f"{key}: {value:.4f} < floor {floor}")
        print(
            f"{key:22} {value:8.4%}  floor {floor:.0%}  {'OK' if ok else 'LOW'}"
        )
    print(
        f"{'cov_licitacao':22} {mod['cov_licitacao']:8.4%}  (reported, not enforced)"
    )

    if failures:
        print("\nFAILED:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("\nMG reconciliation PASSED")


if __name__ == "__main__":
    main()
