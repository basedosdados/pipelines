"""Reconcile the Paraíba rows against their raw staging mirror.

PB needs no pivot -- it publishes all three phase values on one empenho row -- so the
failure modes here are not RS's or SC's. They are:

  * **A key that fans out.** `numeroEmpenho` is an integer that restarts per exercise
    and per unit; on its own it multiplies rows roughly eightfold, and even
    (ano, unidade, numero) collides ~5,700 times a year because unidade codes repeat
    across órgãos. Grain is asserted directly.
  * **A movement type with nowhere to go.** `descricaoTipo` splits the rows into
    PRINCIPAL and three kinds of modification. A fifth value appearing upstream would be
    carried into `despesa` with values that may mean something different, so the
    inventory is a hard gate.
  * **`codigoLicitacao` quietly becoming an identifier.** Today it is a modality with 17
    distinct values, and the model treats it as one. If PB ever repurposed the column,
    reading it as a modality would silently mislabel every row, so its cardinality is
    checked.
  * **The R$1.0bn question this script exists to keep visible.** See below.

**On the unresolved R$1.0bn.** Keeping every empenho document gives one total for the
exercise; keeping only PRINCIPAL gives another, and for 2024 they differ by R$1.02bn.
Either a principal's own `valorAnulado` and the separate ANULAÇÃO document record the
same cancellation twice, or they record different ones. The source does not say. This
script reports BOTH figures on every run rather than asserting one, so the question
stays in front of whoever reads the output instead of being settled by silence.

Usage:
    uv run python models/br_bd_execucao_estadual/code/validate_pb.py [--env dev]
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

# Every movement the model knows how to place.
KNOWN_TIPOS = {
    "PRINCIPAL",
    "ANULAÇÃO PARCIAL",
    "ANULAÇÃO TOTAL",
    "SUPLEMENTAÇÃO",
}

# `codigoLicitacao` is a modality code. 17 distinct values in 2024; a jump to thousands
# would mean PB had turned it into a tender identifier and the model's reading is stale.
MAX_MODALIDADES = 60

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
    print(f"validate_pb -- {PROJECT}\n")

    print("movement inventory (hard gate)")
    seen = {
        str(r["t"]).strip()
        for r in client.query(
            f"select distinct descricaotipo as t from `{STAGING}.pb_empenho`"
        ).result()
        if r["t"] is not None
    }
    check(
        seen <= KNOWN_TIPOS,
        "descricaoTipo",
        f"seen={sorted(seen)}"
        + ("" if seen <= KNOWN_TIPOS else "  <-- UNPLACED MOVEMENT TYPE"),
    )

    print("\nmodality column has not become an identifier")
    card = one(
        client,
        f"select count(distinct codigolicitacao) as n from `{STAGING}.pb_empenho`",
    )
    check(
        card["n"] <= MAX_MODALIDADES,
        "codigoLicitacao cardinality",
        f"{card['n']:,} distinct (modality expected, <= {MAX_MODALIDADES})",
    )

    print("\nvalue identity: valorDespesa == valorEmpenhado + valorAnulado")
    ident = one(
        client,
        f"""
        select countif(abs(
                 safe_cast(valordespesa as float64)
                 - (coalesce(safe_cast(valorempenhado as float64), 0)
                    + coalesce(safe_cast(valoranulado as float64), 0))
               ) > 0.01) as violations,
               count(*) as n
        from `{STAGING}.pb_empenho`
        """,
    )
    share = 100 * ident["violations"] / max(ident["n"], 1)
    check(
        share < 0.5,
        "valorDespesa identity",
        f"{ident['violations']:,} of {ident['n']:,} rows differ ({share:.3f}%)",
    )

    print("\ngrain")
    g = one(
        client,
        f"""
        select count(*) as n, count(distinct id_empenho_bd) as d
        from `{PUB}.despesa` where sigla_uf = 'PB'
        """,
    )
    check(
        g["n"] == g["d"],
        "id_empenho_bd unique",
        f"{g['n']:,} rows, {g['d']:,} distinct",
    )

    fan = one(
        client,
        f"""
        select count(*) as total, count(distinct numeroempenho) as bare
        from `{STAGING}.pb_empenho`
        """,
    )
    notes.append(
        f"numeroEmpenho on its own: {fan['bare']:,} distinct over {fan['total']:,} rows "
        f"(x{fan['total'] / max(fan['bare'], 1):.2f} fan-out -- why the key is composite)"
    )

    print("\nrow count and totals vs staging")
    raw = one(
        client,
        f"""
        select count(*) as n,
               round(sum(safe_cast(valordespesa as float64)), 2) as todos,
               round(sum(case when trim(descricaotipo) = 'PRINCIPAL'
                              then safe_cast(valordespesa as float64) end), 2) as principais
        from `{STAGING}.pb_empenho`
        """,
    )
    model = one(
        client,
        f"""
        select count(*) as n, round(sum(valor_empenhado), 2) as total
        from `{PUB}.despesa` where sigla_uf = 'PB'
        """,
    )
    check(
        model["n"] == raw["n"],
        "row count",
        f"model {model['n']:,} vs staging {raw['n']:,}",
    )
    a, b = raw["todos"] or 0.0, model["total"] or 0.0
    rel = abs(a - b) / max(abs(a), 1.0)
    check(
        rel < 1e-12,
        "valor_empenhado total",
        f"staging {a:,.2f} vs model {b:,.2f}",
    )

    # The open question, reported rather than asserted.
    delta = (raw["principais"] or 0.0) - (raw["todos"] or 0.0)
    notes.append(
        "UNRESOLVED -- every document R$ {:,.2f} vs PRINCIPAL only R$ {:,.2f} "
        "(difference R$ {:,.2f}). The model keeps every document; see the header of "
        "states/br_bd_execucao_estadual__despesa_pb.sql.".format(
            raw["todos"] or 0.0, raw["principais"] or 0.0, delta
        )
    )

    print("\ncreditor document shapes")
    shapes = one(
        client,
        f"""
        select countif(tipo_documento_credor = 'PJ') as pj,
               countif(tipo_documento_credor = 'PF') as pf,
               countif(tipo_documento_credor is null and documento_credor is not null)
                 as unclassified,
               countif(documento_credor is null) as nulos
        from `{PUB}.despesa` where sigla_uf = 'PB'
        """,
    )
    notes.append(
        f"creditor shapes: PJ {shapes['pj']:,}, PF {shapes['pf']:,}, "
        f"unclassified {shapes['unclassified']:,}, null {shapes['nulos']:,}"
    )

    print("\ncoverage")
    cov = one(
        client,
        f"""
        select min(ano) as lo, max(ano) as hi,
               count(distinct format('%d-%02d', ano, mes)) as meses,
               countif(ano is null) as sem_ano
        from `{PUB}.despesa` where sigla_uf = 'PB'
        """,
    )
    check(
        cov["lo"] == 2015 and cov["sem_ano"] == 0,
        "coverage",
        f"{cov['lo']}-{cov['hi']}, {cov['meses']} months, "
        f"{cov['sem_ano']:,} rows without a year",
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
