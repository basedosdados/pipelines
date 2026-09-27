"""Reconcile the harmonized ES rows against their raw staging mirrors.

ES needs a different set of checks from MG. MG's risk is join fan-out through a
dimensional model; ES has no dimensions at all -- `despesa_es` is a straight projection
of one flat table -- so the row count cannot inflate. What CAN go wrong here is:

  * a `safe_cast` silently emptying a column (the BR decimal comma is parsed by
    replacing ',' with '.', and if a thousands separator ever appeared the whole money
    column would go NULL while every dbt test still passed),
  * `parse_date` failing on a format change and nulling `data`/`mes`,
  * the procurement joins losing or multiplying item rows,
  * the tender key on the expense row failing to match `licitacao`.

So the gate is: row counts and money totals against the raw mirror, per-column null
rates on the columns most likely to empty, and the cross-table join rates.

Everything is one pass per table. Issuing a scalar subquery per metric re-scans a
15.3M-row table each time; a MiDES validation written that way billed 63.87 GB.

Usage:
    uv run python models/br_bd_execucao_estadual/code/validate_es.py [--env dev]
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

# Raw side, one scan. `ano` is cast because staging is all-STRING: without it
# '2009' != 2009 and every comparison fails for the wrong reason.
RAW = f"""
select
    count(*) as n_rows,
    count(distinct nullif(trim(DocumentoEmpenho), '')) as n_empenhos,
    round(sum(safe_cast(replace(ValorEmpenho, ',', '.') as float64)), 2) as v_emp,
    round(sum(safe_cast(replace(ValorLiquidado, ',', '.') as float64)), 2) as v_liq,
    round(sum(safe_cast(replace(ValorPago, ',', '.') as float64)), 2) as v_pago,
    min(safe_cast(ano as int64)) as ano_min,
    max(safe_cast(ano as int64)) as ano_max
from `{STAGING}.es_despesa`
"""

MODEL = f"""
select
    count(*) as n_rows,
    count(distinct numero_empenho) as n_empenhos,
    round(sum(valor_empenhado), 2) as v_emp,
    round(sum(valor_liquidado), 2) as v_liq,
    round(sum(valor_pago), 2) as v_pago,
    min(ano) as ano_min,
    max(ano) as ano_max
from `{PUB}.despesa`
where sigla_uf = 'ES'
"""

# Columns whose emptiness would be invisible to dbt tests. Reported as a null RATE so
# a partial failure (one bad exercise) is as visible as a total one.
NULLS = f"""
select
    count(*) as n,
    countif(data is null) / count(*) as data_null,
    countif(mes is null) / count(*) as mes_null,
    countif(valor_empenhado is null) / count(*) as v_emp_null,
    countif(valor_pago is null) / count(*) as v_pago_null,
    countif(nome_credor is null) / count(*) as credor_null,
    countif(documento_credor is null) / count(*) as doc_null,
    countif(id_empenho is null) / count(*) as empenho_null,
    countif(id_licitacao_bd is null) / count(*) as tender_null,
    countif(tipo_documento is null) / count(*) as tipodoc_null
from `{PUB}.despesa`
where sigla_uf = 'ES'
"""

# Does the tender key on the expense row actually reach a tender? A key that matches
# nothing is worse than a null one, because it looks like a link.
JOINS = f"""
with
    d as (
        select distinct id_licitacao_bd
        from `{PUB}.despesa`
        where sigla_uf = 'ES' and id_licitacao_bd is not null
    ),
    l as (select distinct id_licitacao_bd from `{PUB}.licitacao` where sigla_uf = 'ES'),
    i as (
        select count(*) as n, countif(id_licitacao_bd is null) as orphan
        from `{PUB}.licitacao_item`
        where sigla_uf = 'ES'
    ),
    p as (
        select count(*) as n, countif(id_licitacao_bd is null) as orphan
        from `{PUB}.licitacao_participante`
        where sigla_uf = 'ES'
    )
select
    (select count(*) from d) as despesa_tender_keys,
    (select count(*) from d join l using (id_licitacao_bd)) as matched_in_licitacao,
    (select n from i) as item_rows,
    (select orphan from i) as item_orphans,
    (select n from p) as participante_rows,
    (select orphan from p) as participante_orphans
"""

# Winners per (lot, item): the shape that exposed BA's inferred flag, and the reason
# bids here are keyed on the pair rather than the item code alone.
#
# The gate is the RATE, not the maximum. A single group with two winners does not mean
# the key is wrong -- ES genuinely publishes 37 such groups out of 229,310 (0.016%),
# none of which spans an exercise or a tender. What a wrong key looks like is the 2023
# figure computed on `CodigoLoteItem` alone: one group holding 8,871 winners across
# 2,331 tenders. So fail on a mean far from 1 or on a large multi-winner share, and
# report the max as information.
WINNERS = f"""
select
    count(*) as grupos,
    max(vencedores) as max_vencedores,
    round(avg(vencedores), 3) as media_vencedores,
    countif(vencedores > 1) as grupos_multi,
    round(safe_divide(countif(vencedores > 1), count(*)), 5) as taxa_multi
from (
    select id_item_bd, countif(vencedor) as vencedores
    from `{PUB}.licitacao_participante`
    where sigla_uf = 'ES' and id_item_bd is not null
    group by id_item_bd
)
"""


# Every key the ES models join on, and how unique it has to be. A duplicated join key
# does not lose rows -- it MULTIPLIES them, inflating counts and totals through the
# join while every row still looks individually correct.
#
# This is the check that names the offending table. A row-count mismatch only says
# something is wrong; `es_lote` repeating 17 of 243,931 `CodigoLote` values under a
# second surrogate manufactured 52 duplicate items, and nothing else pointed at the lot
# table. Generalised from validate_mg.check_keys.
JOIN_KEYS = [
    ("es_lote", "trim(CodigoLote)"),
    ("es_licitacao", "trim(IdLicitacao)"),
    ("es_licitacao", "nullif(trim(NumeroProcesso), '')"),
    (
        "es_licitacao_item",
        "concat(trim(CodigoLote), '|', trim(CodigoLoteItem))",
    ),
]


def one(client: bigquery.Client, sql: str) -> dict:
    return dict(next(iter(client.query(sql).result())).items())


def check_keys(client: bigquery.Client) -> list[str]:
    """Uniqueness of every join key, reported per table."""
    print("=== join-key uniqueness ===")
    print(f"{'table':26} {'key':46} {'rows':>10} {'distinct':>10}  status")
    bad = []
    for table, key in JOIN_KEYS:
        r = one(
            client,
            f"select count(*) as n, count(distinct {key}) as d "
            f"from `{STAGING}.{table}`",
        )
        ok = r["n"] == r["d"]
        status = "OK" if ok else f"{r['n'] - r['d']:,} DUPLICATED"
        print(f"{table:26} {key:46} {r['n']:>10,} {r['d']:>10,}  {status}")
        if not ok:
            # Not automatically a failure: the models collapse the keys they know are
            # duplicated. Report it so a NEW duplicate is visible.
            bad.append(f"{table}.{key}: {r['n'] - r['d']:,} duplicate values")
    return bad


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    failures: list[str] = []

    # Reported, not fatal: the models deliberately collapse the duplicates they know
    # about. A NEW one shows up here first.
    for note in check_keys(client):
        print(f"  note: {note}")
    print()

    raw, model = one(client, RAW), one(client, MODEL)
    print(f"=== despesa / ES ({ENV}) ===")
    print(f"{'metric':16} {'raw':>22} {'model':>22}  status")
    for key in ("n_rows", "n_empenhos", "ano_min", "ano_max"):
        ok = raw[key] == model[key]
        print(
            f"{key:16} {raw[key]!s:>22} {model[key]!s:>22}  "
            f"{'OK' if ok else 'MISMATCH'}"
        )
        if not ok:
            failures.append(f"{key}: raw {raw[key]} vs model {model[key]}")

    # Money is compared on RELATIVE difference. Summing 15M float64 in a different
    # order moves the last cent, and BigQuery promises no order, so equality here
    # would fail for arithmetic reasons rather than data ones.
    for key in ("v_emp", "v_liq", "v_pago"):
        r, m = raw[key] or 0.0, model[key] or 0.0
        rel = abs(r - m) / max(abs(r), 1.0)
        ok = rel < 1e-12
        print(
            f"{key:16} {r:>22,.2f} {m:>22,.2f}  "
            f"{'OK' if ok else f'MISMATCH rel={rel:.2e}'}"
        )
        if not ok:
            failures.append(f"{key}: relative difference {rel:.2e}")

    print("\n=== null rates (despesa / ES) ===")
    n = one(client, NULLS)
    total = n.pop("n")
    print(f"rows: {total:,}")
    for key, rate in n.items():
        # A fully-null column is always a defect. `tender_null` is expected to be high:
        # only spending that went through a purchase process carries a tender.
        flag = "" if rate < 1.0 else "   <-- COLUMN IS ENTIRELY NULL"
        print(f"  {key:16} {rate:7.2%}{flag}")
        if rate >= 1.0:
            failures.append(f"{key} is 100% null")

    print("\n=== joins ===")
    j = one(client, JOINS)
    keys, matched = j["despesa_tender_keys"], j["matched_in_licitacao"]
    print(f"  despesa tender keys      {keys:,}")
    print(
        f"  ... found in licitacao   {matched:,} ({matched / max(keys, 1):.2%})"
    )
    # `despesa_es` emits the tender key only for processes that exist in `licitacao`,
    # so anything unmatched here is a defect rather than the expected
    # administrative-process majority. Before that semi-join was added, only 6.07% of
    # 948,479 emitted keys resolved -- the rest were process codes for diárias,
    # payroll and transfers that never went through procurement.
    if matched != keys:
        failures.append(
            f"{keys - matched:,} despesa tender keys match no tender; the key must be "
            "emitted only where it resolves"
        )
    for side in ("item", "participante"):
        rows, orph = j[f"{side}_rows"], j[f"{side}_orphans"]
        print(
            f"  {side + ' rows':24} {rows:,}  without a tender: {orph:,} "
            f"({orph / max(rows, 1):.2%})"
        )

    print("\n=== winners per (lot, item) ===")
    w = one(client, WINNERS)
    print(
        f"  groups {w['grupos']:,}  mean {w['media_vencedores']}  "
        f"max {w['max_vencedores']}"
    )
    print(
        f"  groups with >1 winner: {w['grupos_multi']:,} "
        f"({(w['taxa_multi'] or 0):.3%})"
    )
    # BA's published flag gives 0.96 winners per item; its inferred one gave 1.67.
    if not 0.8 <= (w["media_vencedores"] or 0) <= 1.05:
        failures.append(
            f"mean winners per (lot, item) is {w['media_vencedores']}, expected ~1"
        )
    if (w["taxa_multi"] or 0) > 0.005:
        failures.append(
            f"{(w['taxa_multi'] or 0):.3%} of groups have >1 winner -- the bid key is "
            "probably too narrow"
        )

    print()
    if failures:
        print(f"FAILED ({len(failures)}):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
