"""Reconcile the pivoted SC rows against their raw staging mirrors.

Like RS, SC changes the SHAPE of the data: the source publishes one document per
movement and `despesa_sc` pivots to one row per empenho, with the three phases as
columns. Unlike RS, the pivot is two-sided -- within the empenho (emissão + reforço +
anulação + estorno) and across phases (liquidação, pagamento) -- so there are two
independent ways to lose or multiply money.

The checks here are the ones that would have caught the defects actually found while
building SC, plus the two decisions that were established on a single month and must
hold across the whole series:

  * **Movement inventory is a HARD gate.** A fifth `cdtipoempenho` appearing upstream
    would be silently dropped from `valor_empenhado` while the row count stayed
    plausible. Same for a third `cdtipoliquidacao` / fourth `cdtipopagamento`.
  * **Grain.** `nunotaempenho` is NOT unique across unidades gestoras -- 19,533 of
    21,753 numbers in 2011 appear under two or more -- so a join on the bare number
    would multiply rows roughly tenfold. Grain is asserted directly.
  * **Retenção is INCLUDED**, the opposite of RS. That was established on 2024-03, where
    every payment document was Líquido OR Retenção OR Estorno and never a mix. If a
    document ever carried both, Líquido would be gross and the model would double-count,
    so the no-mix property is re-checked over every month.
  * **Unresolved modifications.** A reforço/anulação/estorno whose original is not in the
    staging mirror creates a group with no emissão, and its dimensions and date would be
    taken from the modification instead. 100% resolved within 2011; across the series,
    restos a pagar make some point at earlier exercises, so this is reported with a
    threshold rather than asserted at zero.

Usage:
    uv run python models/br_bd_execucao_estadual/code/validate_sc.py [--env dev]
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
PUB = f"{PROJECT}.{DATASET}"

# Every movement the model knows how to place. Anything else is money with nowhere to go.
KNOWN_EMPENHO = {"1", "2", "3", "4"}
KNOWN_LIQUIDACAO = {"1", "2"}
KNOWN_PAGAMENTO = {"1", "2", "3"}

# Comma decimal, no thousands separator. Written once so no check can disagree with
# another about what a value is.
NUM = "safe_cast(replace({col}, ',', '.') as float64)"

failures: list[str] = []
notes: list[str] = []


def check(ok: bool, label: str, detail: str) -> None:
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}: {detail}", flush=True)
    if not ok:
        failures.append(f"{label}: {detail}")


def one(client: bigquery.Client, sql: str) -> dict:
    return dict(next(iter(client.query(sql).result())).items())


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    print(f"validate_sc -- {PROJECT}\n")

    # ---------------------------------------------------------------- inventories
    print("movement inventories (hard gate)")
    for table, col, known in [
        ("sc_empenho", "cdtipoempenho", KNOWN_EMPENHO),
        ("sc_liquidacao", "cdtipoliquidacao", KNOWN_LIQUIDACAO),
        ("sc_pagamento", "cdtipopagamento", KNOWN_PAGAMENTO),
    ]:
        seen = {
            str(r["t"])
            for r in client.query(
                f"select distinct trim({col}) as t from `{STAGING}.{table}`"
            ).result()
            if r["t"] is not None
        }
        check(
            seen <= known,
            f"{table}.{col}",
            f"seen={sorted(seen)} known={sorted(known)}"
            + ("" if seen <= known else "  <-- UNPLACED MOVEMENT TYPE"),
        )

    # ---------------------------------------------------------------- money
    print("\nempenho reconciliation")
    raw = one(
        client,
        f"""
        select
          count(*) as rows_raw,
          count(distinct case when trim(cdtipoempenho) = '1' then trim(ugempenho) end)
            as emissoes,
          round(sum({NUM.format(col="vlempenho")}), 2) as total_raw
        from `{STAGING}.sc_empenho`
        """,
    )
    model = one(
        client,
        f"""
        select count(*) as rows_model,
               round(sum(valor_empenhado), 2) as total_model
        from `{PUB}.despesa` where sigla_uf = 'SC'
        """,
    )
    # Relative, never equality: summing millions of float64 in a different order moves
    # the last cent and BigQuery promises no order.
    a, b = raw["total_raw"] or 0.0, model["total_model"] or 0.0
    rel = abs(a - b) / max(abs(a), 1.0)
    check(
        rel < 1e-12,
        "valor_empenhado total",
        f"raw {a:,.2f} vs model {b:,.2f} (rel {rel:.2e})",
    )
    check(
        model["rows_model"] == raw["emissoes"],
        "row count = distinct emissões",
        f"model {model['rows_model']:,} vs emissões {raw['emissoes']:,}",
    )

    for phase, table, col in [
        ("valor_liquidado", "sc_liquidacao", "vlliquidacao"),
        ("valor_pago", "sc_pagamento", "vlpagamento"),
    ]:
        r = one(
            client,
            f"""
            select round(sum({NUM.format(col=col)}), 2) as t
            from `{STAGING}.{table}`
            where nullif(trim(ugempenhooriginal), '') is not null
            """,
        )
        m = one(
            client,
            f"select round(sum({phase}), 2) as t from `{PUB}.despesa` where sigla_uf = 'SC'",
        )
        a, b = r["t"] or 0.0, m["t"] or 0.0
        rel = abs(a - b) / max(abs(a), 1.0)
        # Not an equality gate: a document whose empenho is absent from the mirror
        # (committed before 2011) has nowhere to land, so the model can be lower.
        check(
            b <= a * (1 + 1e-12),
            f"{phase} <= staging total",
            f"staging {a:,.2f} vs model {b:,.2f} (rel {rel:.2e})",
        )

    # ---------------------------------------------------------------- grain
    print("\ngrain")
    g = one(
        client,
        f"""
        select count(*) as n, count(distinct id_empenho_bd) as d
        from `{PUB}.despesa` where sigla_uf = 'SC'
        """,
    )
    check(
        g["n"] == g["d"],
        "id_empenho_bd unique",
        f"{g['n']:,} rows, {g['d']:,} distinct",
    )

    dup = one(
        client,
        f"""
        select count(*) as n from (
          select trim(nunotaempenho) as k
          from `{STAGING}.sc_empenho`
          group by 1 having count(distinct trim(cdunidadegestora)) > 1)
        """,
    )
    notes.append(
        f"empenho numbers reused across UGs: {dup['n']:,} "
        f"(expected non-zero -- this is why joins use the composite key)"
    )

    # ---------------------------------------------------------------- retenção
    print("\nretenção (the RS divergence)")
    mixed = one(
        client,
        f"""
        select count(*) as n from (
          select trim(cdunidadegestora) as ug, trim(nupagamento) as doc
          from `{STAGING}.sc_pagamento`
          group by 1, 2 having count(distinct trim(nmtipopagamento)) > 1)
        """,
    )
    check(
        mixed["n"] == 0,
        "no payment document mixes types",
        f"{mixed['n']:,} mixed documents"
        + (
            ""
            if mixed["n"] == 0
            else "  <-- Líquido may be GROSS; including Retenção would double-count"
        ),
    )

    # ---------------------------------------------------------------- linkage
    print("\nlinkage")
    unresolved = one(
        client,
        f"""
        with emissao as (
          select distinct trim(ugempenho) as k
          from `{STAGING}.sc_empenho` where trim(cdtipoempenho) = '1'),
        mods as (
          select trim(ugempenhooriginal) as k
          from `{STAGING}.sc_empenho`
          where trim(cdtipoempenho) <> '1'
            and nullif(trim(nunotaempenhooriginal), '') is not null)
        select count(*) as total,
               countif(k not in (select k from emissao)) as orphan
        from mods
        """,
    )
    share = 100 * unresolved["orphan"] / max(unresolved["total"], 1)
    check(
        share < 5.0,
        "modifications resolving to an emissão",
        f"{unresolved['total'] - unresolved['orphan']:,}/{unresolved['total']:,} "
        f"({100 - share:.2f}%) -- orphans are commitments from before 2011",
    )

    # ---------------------------------------------------------------- creditors
    print("\ncreditor document shapes")
    shapes = one(
        client,
        f"""
        select
          countif(documento_credor like '%/%') as pj,
          countif(documento_credor like '%*%') as pf,
          countif(documento_credor is not null
                  and documento_credor not like '%/%'
                  and documento_credor not like '%*%') as outros,
          countif(documento_credor is null) as nulos
        from `{PUB}.despesa` where sigla_uf = 'SC'
        """,
    )
    notes.append(
        f"creditor shapes: PJ {shapes['pj']:,}, PF {shapes['pf']:,}, "
        f"unclassified {shapes['outros']:,}, null {shapes['nulos']:,}"
    )

    # ---------------------------------------------------------------- coverage
    print("\ncoverage")
    cov = one(
        client,
        f"""
        select min(ano) as lo, max(ano) as hi,
               count(distinct format('%d-%02d', ano, mes)) as meses,
               countif(ano is null) as sem_ano
        from `{PUB}.despesa` where sigla_uf = 'SC'
        """,
    )
    check(
        cov["lo"] == 2011 and cov["sem_ano"] == 0,
        "coverage",
        f"{cov['lo']}-{cov['hi']}, {cov['meses']} months, {cov['sem_ano']:,} rows without a year",
    )

    print()
    for n in notes:
        print(f"  [note] {n}")
    if failures:
        print(f"\n{len(failures)} CHECK(S) FAILED")
        for f in failures:
            print(f"  - {f}")
        raise SystemExit(1)
    print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    main()
