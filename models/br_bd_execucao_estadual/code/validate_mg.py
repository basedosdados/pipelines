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
    min(ano) as ano_min,
    max(ano) as ano_max
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


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    raw = next(iter(client.query(RAW).result()))
    mod = next(iter(client.query(MODEL).result()))

    failures: list[str] = []

    print("=== MG: harmonized despesa vs raw ft_despesa ===")
    print(f"{'metric':22} {'raw':>22} {'model':>22}  status")
    for key in (
        "n_rows",
        "n_empenhos",
        "vr_empenhado",
        "vr_liquidado",
        "vr_pago",
    ):
        r, m = raw[key], mod[key]
        ok = r == m
        # A LEFT JOIN cannot drop rows, so any difference is a fan-out or a lost cast --
        # either way it is a defect, and an exact match is the right bar.
        if not ok:
            failures.append(f"{key}: raw {r!r} != model {m!r}")
        print(f"{key:22} {r!s:>22} {m!s:>22}  {'OK' if ok else 'MISMATCH'}")

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
