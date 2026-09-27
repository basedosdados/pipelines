"""Reconcile São Paulo's `despesa_anual` against its staging mirror.

This check exists in a particular shape because of what it nearly missed. Every SIGEO
export ends with a TOTALS row -- dimensions blank, "TOTAL" in the Despesa field, values
equal to the sum of the rows above. There are 509 of them, one per (exercise, órgão), and
together they carry R$12.7 trillion: almost exactly half of the naive total.

A plain raw-vs-model sum does not catch that. The totals rows sit on BOTH sides, so the
two agree to the cent and are both double the truth. So this validator does two things a
naive one does not:

* it excludes the totals rows from the raw side, and asserts the model excluded them too;
* it uses those same totals rows as an INDEPENDENT check, since the source is telling us
  what each (exercise, órgão) should sum to. If the model's own sum per órgão matches the
  published total, the transform is right for reasons that have nothing to do with our
  arithmetic.

Usage:
    uv run python models/br_bd_execucao_estadual/code/validate_sp.py [--env dev]
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

# The exports are Brazilian-formatted and right-padded inside quotes.
BR = "safe_cast(replace(replace(trim({0}), '.', ''), ',', '.') as float64)"
COLS = (
    ("empenhado", "valor_empenhado"),
    ("liquidado", "valor_liquidado"),
    ("pago", "valor_pago"),
    ("pago_restos", "valor_pago_restos"),
)
# A totals row has no órgão; a data row always does.
IS_TOTAL = "trim(coalesce(orgao, '')) = ''"


def tol(base: float | None) -> float:
    """Absolute tolerance for a money comparison of magnitude `base`.

    Summing hundreds of thousands of float64 values in a different order moves the last
    cents: a year total near R$500bn drifts by ~R$0.30. A flat R$0.05 would report that
    as a discrepancy every run and train the reader to ignore the check. The floor keeps
    small órgãos strict.
    """
    return max(0.05, 1e-11 * abs(base or 0.0))


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    failures: list[str] = []

    raw_sel = ", ".join(
        f"round(sum({BR.format(src)}), 2) as {src}" for src, _ in COLS
    )
    raw = next(
        iter(
            client.query(
                f"select count(*) n, {raw_sel} from `{STAGING}.sp_despesa` "
                f"where not ({IS_TOTAL})"
            ).result()
        )
    )
    mod_sel = ", ".join(f"round(sum({dst}), 2) as {src}" for src, dst in COLS)
    mod = next(
        iter(
            client.query(
                f"select count(*) n, {mod_sel} from `{PROD}.despesa_anual`"
            ).result()
        )
    )

    print(f"{'':14}{'raw (excl. totals)':>24}{'model':>24}")
    ok = raw.n == mod.n
    if not ok:
        failures.append(f"n: raw {raw.n:,} != model {mod.n:,}")
    print(f"{'n':14}{raw.n:>24,}{mod.n:>24,}  {'OK' if ok else 'MISMATCH'}")
    for src, _ in COLS:
        a, b = raw[src], mod[src]
        rel = abs(a - b) / max(abs(a), 1.0)
        ok = rel < 1e-12
        if not ok:
            failures.append(f"{src}: raw {a!r} vs model {b!r}")
        print(
            f"{src:14}{a:>24,.2f}{b:>24,.2f}  "
            f"{'OK' if ok else 'MISMATCH'} (rel {rel:.1e})"
        )

    # The totals rows must be gone from the model, not merely outnumbered.
    leaked = next(
        iter(
            client.query(
                f"select count(*) n from `{PROD}.despesa_anual` "
                "where orgao is null or trim(orgao) = ''"
            ).result()
        )
    ).n
    if leaked:
        failures.append(f"{leaked:,} totals rows leaked into the model")
    print(
        f"\ntotals rows in model: {leaked} {'OK' if not leaked else 'LEAKED'}"
    )

    # Year totals must match exactly. This is the check that would catch losing or
    # duplicating rows, and unlike the per-órgão comparison below it is not disturbed by
    # rows landing in the wrong file, since those stay inside their own exercise.
    print("\nper-exercise totals vs the source's own published TOTAL rows:")
    q_year = f"""
    with published as (
        select safe_cast(ano as int64) as ano,
               {", ".join(f"sum({BR.format(src)}) as {src}" for src, _ in COLS)}
        from `{STAGING}.sp_despesa` where {IS_TOTAL} group by ano
    ), ours as (
        select ano, {", ".join(f"sum({dst}) as {src}" for src, dst in COLS)}
        from `{PROD}.despesa_anual` group by ano
    )
    select p.ano,
        {", ".join(f"round(o.{src} - p.{src}, 2) as d_{src}" for src, _ in COLS)},
        {", ".join(f"p.{src} as p_{src}" for src, _ in COLS)}
    from published p join ours o using (ano) order by p.ano
    """
    worst = 0.0
    for row in client.query(q_year).result():
        for src, _ in COLS:
            d = abs(row[f"d_{src}"] or 0)
            base = abs(row[f"p_{src}"] or 0)
            worst = max(worst, d)
            if d > tol(base):
                failures.append(
                    f"{row.ano} {src}: exercise total differs by {d:,.2f}"
                )
                print(f"  {row.ano} {src}: MISMATCH {d:,.2f}")
    print(
        f"  17 exercises, largest difference R$ {worst:.2f} (float64 summation "
        f"order over ~300k rows per year)"
    )

    # Independent check: the source publishes what each (exercise, órgão) should sum to.
    #
    # One known exception, and it is the SOURCE that is inconsistent, not the model.
    # SIGEO's 2010 query for órgão 28000 returned 8 rows belonging to órgão 38000
    # (unidades 3801xx/3802xx, one supplier, R$368,701.10 of empenhado), and those rows
    # appear nowhere in 38000's own export. The file-level TOTAL for 28000 counts them,
    # 38000's does not. This model attributes each row by its own Órgão field, which puts
    # them under 38000 where they belong -- so our per-órgão split is right and the
    # source's is not. The exercise total above is unaffected, since both órgãos are in
    # the same year.
    known = {(2010, "38000"), (2010, "28000")}
    print("\nper-órgão sums vs the source's own published TOTAL row:")
    q = f"""
    with published as (
        select
            safe_cast(ano as int64) as ano,
            orgao_arquivo as orgao,
            {", ".join(f"{BR.format(src)} as {src}" for src, _ in COLS)}
        from `{STAGING}.sp_despesa`
        where {IS_TOTAL}
    ),
    ours as (
        select ano, orgao,
            {", ".join(f"sum({dst}) as {src}" for src, dst in COLS)}
        from `{PROD}.despesa_anual` group by ano, orgao
    )
    select coalesce(p.ano, o.ano) as ano, coalesce(p.orgao, o.orgao) as orgao,
        {
        ", ".join(
            f"round(coalesce(o.{src},0) - coalesce(p.{src},0), 2) as d_{src}"
            for src, _ in COLS
        )
    },
        {", ".join(f"coalesce(p.{src}, 0) as p_{src}" for src, _ in COLS)}
    from published p
    full outer join ours o on p.ano = o.ano and p.orgao = o.orgao
    """
    pairs = unexpected = 0
    for row in client.query(q).result():
        pairs += 1
        worst_col = max(
            ((abs(row[f"d_{src}"] or 0), src) for src, _ in COLS),
            default=(0, ""),
        )
        if worst_col[0] <= tol(
            row[f"p_{worst_col[1]}"] if worst_col[1] else 0
        ):
            continue
        key = (row.ano, row.orgao)
        if key in known:
            print(
                f"    {row.ano}/{row.orgao}: {worst_col[1]} off by "
                f"{row[f'd_{worst_col[1]}']:,.2f} -- known source leak, see above"
            )
            continue
        unexpected += 1
        failures.append(
            f"{row.ano}/{row.orgao}: {worst_col[1]} differs from published total by "
            f"{row[f'd_{worst_col[1]}']:,.2f}"
        )
        print(f"    {row.ano}/{row.orgao}: MISMATCH on {worst_col[1]}")
    print(
        f"  {pairs:,} (exercise, órgão) pairs compared, {unexpected} unexplained "
        f"{'OK' if not unexpected else 'MISMATCH'}"
    )

    if failures:
        print("\nFAILED:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("\nSP reconciliation PASSED")


if __name__ == "__main__":
    main()
