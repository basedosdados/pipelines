"""Reconcile Pernambuco's slice of `despesa` against its staging mirrors.

PE is split across two staging tables because its export changed schema twice, and the two
eras share almost no column names. The raw side therefore has to be summed per table --
there is no single column spanning both -- and each era parsed the way its own model parses
it. Getting that wrong is not hypothetical: the first run of this check reported a mismatch
because the raw side cast the Brazilian-formatted legacy values as US, so it was measuring
the validator rather than the model.

Everything is computed in one pass per table; see validate_mg.py for why that matters.

Usage:
    uv run python models/br_bd_execucao_estadual/code/validate_pe.py [--env dev]
"""

from __future__ import annotations

import sys
import warnings

warnings.filterwarnings("ignore")

from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
ENV = _argv[_argv.index("--env") + 1] if "--env" in _argv else "dev"

PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET = "br_bd_execucao_estadual"
STAGING = f"{PROJECT}.{DATASET}_staging"
PROD = f"{PROJECT}.{DATASET}"

# 2011-2026: plain US numbers (" 43200.0"), so a trim is enough.
RAW_MODERN = f"""
select
    count(*) as n,
    round(sum(safe_cast(trim(vlrempenhado) as float64)), 2) as emp,
    round(sum(safe_cast(trim(vlrliquidado) as float64)), 2) as liq,
    round(sum(safe_cast(trim(vlrtotalpago) as float64)), 2) as pago
from `{STAGING}.pe_despesa`
"""

# 2008-2010: Brazilian numbers ("1.353,96"), and each value lives under whichever of the
# two legacy spellings that exercise used.
RAW_LEGACY = f"""
with v as (
    select
        replace(
            replace(trim(coalesce(valor_empenhado, empenhado)), '.', ''), ',', '.'
        ) as emp,
        replace(
            replace(trim(coalesce(valor_liquidado, liquidado)), '.', ''), ',', '.'
        ) as liq,
        replace(
            replace(trim(coalesce(valor_pago, pago)), '.', ''), ',', '.'
        ) as pago
    from `{STAGING}.pe_despesa_legado`
)
select
    count(*) as n,
    round(sum(safe_cast(emp as float64)), 2) as emp,
    round(sum(safe_cast(liq as float64)), 2) as liq,
    round(sum(safe_cast(pago as float64)), 2) as pago
from v
"""

MODEL = f"""
select
    count(*) as n,
    round(sum(valor_empenhado), 2) as emp,
    round(sum(valor_liquidado), 2) as liq,
    round(sum(valor_pago), 2) as pago
from `{PROD}.despesa`
where sigla_uf = 'PE'
"""

# Per exercise, so an era that parsed to nothing is visible. A whole schema era can land
# present-and-empty without the row count moving at all.
COVERAGE = f"""
select
    ano,
    count(*) as n,
    countif(numero_empenho is not null) / count(*) as cov_empenho,
    countif(nome_credor is not null) / count(*) as cov_credor,
    countif(data is not null) / count(*) as cov_data,
    countif(funcao is not null) / count(*) as cov_funcao,
    countif(valor_empenhado is not null) / count(*) as cov_valor
from `{PROD}.despesa`
where sigla_uf = 'PE'
group by ano
order by ano
"""


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    modern = next(iter(client.query(RAW_MODERN).result()))
    legacy = next(iter(client.query(RAW_LEGACY).result()))
    model = next(iter(client.query(MODEL).result()))

    raw = {
        k: (modern[k] or 0) + (legacy[k] or 0)
        for k in ("n", "emp", "liq", "pago")
    }
    print(f"raw modern {modern['n']:,} rows + legacy {legacy['n']:,} rows")
    print(f"\n{'metric':10} {'raw':>24} {'model':>24}")

    failures: list[str] = []
    r, m = raw["n"], model["n"]
    ok = r == m
    if not ok:
        failures.append(f"n: raw {r:,} != model {m:,}")
    print(f"{'n':10} {r:>24,} {m:>24,}  {'OK' if ok else 'MISMATCH'}")

    # Relative, not exact: summing millions of float64 in a different order moves the last
    # cent. See validate_mg.py.
    for key in ("emp", "liq", "pago"):
        r, m = raw[key], model[key]
        rel = abs(r - m) / max(abs(r), 1.0)
        ok = rel < 1e-12
        if not ok:
            failures.append(
                f"{key}: raw {r!r} vs model {m!r} (relative {rel:.2e})"
            )
        print(
            f"{key:10} {r:>24,.2f} {m:>24,.2f}  "
            f"{'OK' if ok else 'MISMATCH'} (rel {rel:.1e})"
        )

    print(f"\n{'ano':6} {'rows':>10}  empenho credor  data  funcao  valor")
    for row in client.query(COVERAGE).result():
        flag = "  <-- EMPTY" if row.cov_valor < 0.5 else ""
        print(
            f"{row.ano:<6} {row.n:>10,}  {row.cov_empenho:.3f}   "
            f"{row.cov_credor:.3f}  {row.cov_data:.3f}  {row.cov_funcao:.3f}   "
            f"{row.cov_valor:.3f}{flag}"
        )
        if row.cov_valor < 0.5:
            failures.append(
                f"{row.ano}: only {row.cov_valor:.1%} of rows carry a value"
            )

    if failures:
        print("\nFAILED:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("\nPE reconciliation PASSED")


if __name__ == "__main__":
    main()
