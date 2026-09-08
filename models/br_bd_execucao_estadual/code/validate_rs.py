"""Reconcile the pivoted RS rows against their raw staging mirror.

RS is the only state whose model changes the SHAPE of the data: the source publishes one
row per phase and `despesa_rs` pivots it to one row per empenho x budget line, with the
phases as columns. That introduces failure modes the other states do not have, so the
checks here are different from validate_es:

  * A pivot can drop money silently. Any `FaseGasto` the model does not name simply
    vanishes from all three value columns, and the row count still looks plausible. The
    phase inventory below is therefore a HARD gate: a seventh phase appearing upstream
    must fail this script, not be quietly excluded.
  * A pivot can multiply rows if the grouping key is wrong. Grain is checked directly.
  * `Retenção` is excluded on purpose, so its exclusion is asserted rather than assumed
    -- if it ever leaked into `valor_pago` the total would move by R$27.5bn.

Usage:
    uv run python models/br_bd_execucao_estadual/code/validate_rs.py [--env dev]
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

# Every phase the model knows how to place. Anything else is money with nowhere to go.
KNOWN_PHASES = {
    "Empenho": "valor_empenhado",
    "Prescrição de Empenho": "valor_empenhado",
    "Liquidação": "valor_liquidado",
    "Prescrição de Liquidação": "valor_liquidado",
    "Pagamento": "valor_pago",
    "Retenção": "(excluded: withheld within a payment, not extra expenditure)",
    # 270 rows carry no phase at all (-R$33.1M). Listed explicitly as a KNOWN
    # exclusion rather than special-cased out of the check: printing "UNKNOWN" while
    # passing is the worst of both, and a growing count here is worth seeing.
    None: "(excluded: no phase published, cannot be attributed)",
}

PHASES = f"""
select
    trim(fasegasto) as fase,
    count(*) as n,
    round(sum(safe_cast(replace(valor, ',', '.') as float64)), 2) as total
from `{STAGING}.rs_despesa`
group by 1
"""

RAW = f"""
select
    count(distinct nullif(trim(empenho), '')) as n_empenhos,
    round(sum(if(trim(fasegasto) in ('Empenho', 'Prescrição de Empenho'),
        safe_cast(replace(valor, ',', '.') as float64), 0)), 2) as v_emp,
    round(sum(if(trim(fasegasto) in ('Liquidação', 'Prescrição de Liquidação'),
        safe_cast(replace(valor, ',', '.') as float64), 0)), 2) as v_liq,
    round(sum(if(trim(fasegasto) = 'Pagamento',
        safe_cast(replace(valor, ',', '.') as float64), 0)), 2) as v_pago
from `{STAGING}.rs_despesa`
where nullif(trim(empenho), '') is not null
"""

MODEL = f"""
select
    count(*) as n_rows,
    count(distinct id_empenho) as n_empenhos,
    round(sum(valor_empenhado), 2) as v_emp,
    round(sum(valor_liquidado), 2) as v_liq,
    round(sum(valor_pago), 2) as v_pago,
    min(ano) as ano_min,
    max(ano) as ano_max
from `{PUB}.despesa`
where sigla_uf = 'RS'
"""

GRAIN = f"""
select
    count(*) as linhas,
    count(distinct id_empenho) as empenhos,
    countif(n > 1) as empenhos_multi_linha
from (
    select id_empenho, count(*) as n
    from `{PUB}.despesa` where sigla_uf = 'RS' group by id_empenho
)
"""

NULLS = f"""
select
    count(*) as n,
    countif(data is null) / count(*) as data_null,
    countif(mes is null) / count(*) as mes_null,
    countif(nome_credor is null) / count(*) as credor_null,
    countif(documento_credor is null) / count(*) as doc_null,
    countif(tipo_documento_credor is null) / count(*) as tipo_doc_null,
    countif(descricao is null) / count(*) as descricao_null,
    countif(modalidade_licitacao is null) / count(*) as modalidade_null
from `{PUB}.despesa`
where sigla_uf = 'RS'
"""

# The six months absent from RS's catalogue. Pinned so a SEVENTH gap is a failure
# rather than a shrug -- nothing else in the pipeline can see a missing month.
KNOWN_GAPS = {(2020, 6), (2020, 8), (2022, 4), (2023, 2), (2023, 6), (2023, 8)}

MONTHS = f"""
select distinct ano, mes from `{PUB}.despesa`
where sigla_uf = 'RS' and ano is not null and mes is not null
"""


def one(client: bigquery.Client, sql: str) -> dict:
    return dict(next(iter(client.query(sql).result())).items())


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    failures: list[str] = []

    print(f"=== phases present in staging ({ENV}) ===")
    seen = {}
    for r in client.query(PHASES).result():
        seen[r.fase] = (r.n, r.total)
        placement = KNOWN_PHASES.get(
            r.fase, "!!! UNKNOWN -- money with nowhere to go"
        )
        print(
            f"  {r.fase!s:26} {r.n:>12,}  R$ {r.total:>18,.2f}  -> {placement}"
        )
        if r.fase not in KNOWN_PHASES:
            failures.append(
                f"unknown FaseGasto {r.fase!r} ({r.n:,} rows, R$ {r.total:,.2f}) is "
                "excluded from every value column by the pivot"
            )

    sem_fase = seen.get(None, (0, 0.0))[0]
    if sem_fase > 1000:
        failures.append(
            f"{sem_fase:,} rows carry no FaseGasto (was 270 at onboarding); that is "
            "money the pivot cannot place"
        )

    raw, model = one(client, RAW), one(client, MODEL)
    print(f"\n=== despesa / RS ({ENV}) ===")
    print(f"{'metric':16} {'raw':>22} {'model':>22}  status")
    ok = raw["n_empenhos"] == model["n_empenhos"]
    print(
        f"{'n_empenhos':16} {raw['n_empenhos']!s:>22} {model['n_empenhos']!s:>22}  "
        f"{'OK' if ok else 'MISMATCH'}"
    )
    if not ok:
        failures.append(
            f"n_empenhos: raw {raw['n_empenhos']} vs model {model['n_empenhos']}"
        )

    # Tolerance is looser than validate_es's 1e-12 for a specific reason: the pivot
    # regroups 50.6M float64 additions, and summation ORDER alone moves the total by
    # ~R$8 on R$887bn (8.7e-12). Verified exact under DECIMAL(20,2) locally, so the
    # residue here is representation, not data.
    for key, label in (
        ("v_emp", "valor_empenhado"),
        ("v_liq", "valor_liquidado"),
        ("v_pago", "valor_pago"),
    ):
        r, m = raw[key] or 0.0, model[key] or 0.0
        rel = abs(r - m) / max(abs(r), 1.0)
        ok = rel < 1e-9
        print(
            f"{label:16} {r:>22,.2f} {m:>22,.2f}  "
            f"{'OK' if ok else f'MISMATCH rel={rel:.2e}'}"
        )
        if not ok:
            failures.append(f"{label}: relative difference {rel:.2e}")

    span = f"{model['ano_min']}-{model['ano_max']}"
    print(f"{'ano span':16} {'':>22} {span:>22}")

    print("\n=== grain ===")
    g = one(client, GRAIN)
    print(f"  rows {g['linhas']:,}  distinct empenhos {g['empenhos']:,}")
    print(
        f"  empenhos on >1 budget line: {g['empenhos_multi_linha']:,} "
        f"({g['empenhos_multi_linha'] / max(g['empenhos'], 1):.3%})"
    )
    # Measured at 222 of 14,763,432. A pivot with a wrong key would blow this up.
    if g["empenhos_multi_linha"] > g["empenhos"] * 0.001:
        failures.append(
            f"{g['empenhos_multi_linha']:,} empenhos span >1 row -- the pivot key is "
            "probably wrong"
        )

    print("\n=== null rates ===")
    n = one(client, NULLS)
    total = n.pop("n")
    print(f"rows: {total:,}")
    for key, rate in n.items():
        flag = "   <-- ENTIRELY NULL" if rate >= 1.0 else ""
        print(f"  {key:20} {rate:7.2%}{flag}")
        if rate >= 1.0:
            failures.append(f"{key} is 100% null")

    print("\n=== calendar coverage ===")
    have = {(r.ano, r.mes) for r in client.query(MONTHS).result()}
    lo, hi = min(have), max(have)
    expected = {
        (y, m)
        for y in range(lo[0], hi[0] + 1)
        for m in range(1, 13)
        if lo <= (y, m) <= hi
    }
    gaps = sorted(expected - have)
    print(
        f"  months present {len(have)}  span {lo[0]}-{lo[1]:02d}..{hi[0]}-{hi[1]:02d}"
    )
    print(f"  gaps: {[f'{y}-{m:02d}' for y, m in gaps] or 'none'}")
    new_gaps = set(gaps) - KNOWN_GAPS
    if new_gaps:
        failures.append(
            f"new month gap(s) not present at onboarding: "
            f"{[f'{y}-{m:02d}' for y, m in sorted(new_gaps)]}"
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
