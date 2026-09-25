"""Reproduce the MG row of Table 3 (and the procurement table) from the WBER paper.

Dahis, Ricca, Scot, Sales & Nascimento, "Fiscal Capacity and Execution at the
Local Level", *World Bank Economic Review* 2026. The published MG figures cover
**2014-2021**; this recomputes them from whichever project/dataset is pointed at,
so the same script answers two different questions:

  * `--max-year 2021` against **prod** -> must reproduce the paper exactly. That
    is the control: if it does not, the definitions here are wrong and nothing
    else in the output means anything.
  * `--max-year 2021` against **dev** -> how the re-harvest changes the published
    window (source revisions, the quoting fix, the 2017/2018 union).
  * no `--max-year` against **dev** -> the extended 2014-2026 statistics.

Every definition is lifted verbatim from the paper's own queries
(`code/archive/descriptive_statistics_execution.ipynb` in the MiDES paper
repository), not reconstructed from the table. The ones worth stating because
they are not guessable:

  * "Positive (%)" divides by COUNT(*), but counts only rows that are positive
    AND have a non-null `id_empenho_bd` -- so it blends a value filter with a
    key filter.
  * "Procurement-related" keys off `SUBSTR(elemento_despesa, 5, 2) IN
    ('30','32','52')` and is a share of DISTINCT commitments, not of rows.
  * "With verification/payment" are distinct-key coverage ratios computed over a
    LEFT JOIN, not row counts.
  * "Distinct sellers" is `COUNT(DISTINCT CONCAT(documento_credor, nome_credor))`
    -- the pair, not the document alone.

The paper filters `RS` to `ano > 2009`; irrelevant for MG and dropped here.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account

# The MG row of Table 3, and of the procurement table, as published.
PAPER = {
    "n_emp": 39015657,
    "pct_positive": 96.4589651790306,
    "pct_procure": 30.2609975272235,
    "pct_with_liq": 95.0,
    "pct_with_pag": 89.0,
    "n_liq": 63753322,
    "n_pag": 64127137,
    "n_sellers": 1710005,
    "total_payment_bn": 514.880276953836,
}


def client(project: str) -> bigquery.Client:
    path = Path.home() / ".basedosdados/credentials/staging.json"
    if path.exists():
        creds = service_account.Credentials.from_service_account_file(
            str(path)
        )
        return bigquery.Client(credentials=creds, project=project)
    return bigquery.Client(project=project)


def run(cli: bigquery.Client, sql: str) -> dict:
    return dict(next(iter(cli.query(sql).result())))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="basedosdados")
    parser.add_argument("--dataset", default="world_wb_mides")
    parser.add_argument("--billing", default="pessoal-rd")
    parser.add_argument("--max-year", type=int)
    parser.add_argument("--uf", default="MG")
    args = parser.parse_args()

    base = f"`{args.project}.{args.dataset}`"
    uf = args.uf
    span = f"AND ano <= {args.max_year}" if args.max_year else ""
    cli = client(args.billing)

    emp = run(
        cli,
        f"""
        SELECT COUNT(*) AS obs,
               SUM(CASE WHEN valor_final > 0 AND id_empenho_bd IS NOT NULL
                        THEN 1 ELSE 0 END) AS positives,
               COUNT(DISTINCT id_municipio) AS municipios,
               MIN(ano) AS y0, MAX(ano) AS y1
        FROM {base}.empenho WHERE sigla_uf='{uf}' {span}""",
    )

    # Distinct-key coverage: procurement share, and verification/payment coverage.
    cov = run(
        cli,
        f"""
        WITH e AS (SELECT * FROM {base}.empenho
                   WHERE sigla_uf='{uf}' AND id_empenho_bd IS NOT NULL {span}),
             l AS (SELECT DISTINCT id_empenho_bd FROM {base}.liquidacao
                   WHERE sigla_uf='{uf}' AND id_empenho_bd IS NOT NULL {span}),
             p AS (SELECT DISTINCT id_empenho_bd FROM {base}.pagamento
                   WHERE sigla_uf='{uf}' AND id_empenho_bd IS NOT NULL {span})
        SELECT COUNT(DISTINCT e.id_empenho_bd) AS distinct_commitments,
               COUNT(DISTINCT CASE WHEN SUBSTR(e.elemento_despesa,5,2)
                                        IN ('30','32','52')
                                   THEN e.id_empenho_bd END) AS procurement,
               COUNT(DISTINCT l.id_empenho_bd) AS with_liq,
               COUNT(DISTINCT p.id_empenho_bd) AS with_pag
        FROM e LEFT JOIN l ON l.id_empenho_bd = e.id_empenho_bd
               LEFT JOIN p ON p.id_empenho_bd = e.id_empenho_bd""",
    )

    liq = run(
        cli,
        f"""
        SELECT COUNT(*) AS obs,
               COUNT(DISTINCT id_liquidacao_bd) AS distinct_verifications
        FROM {base}.liquidacao WHERE sigla_uf='{uf}' {span}""",
    )

    pag = run(
        cli,
        f"""
        SELECT COUNT(*) AS obs,
               COUNT(DISTINCT id_pagamento_bd) AS distinct_payments,
               COUNT(DISTINCT CONCAT(documento_credor, nome_credor)) AS sellers,
               SUM(valor_final)/1e9 AS nominal_bn
        FROM {base}.pagamento WHERE sigla_uf='{uf}' {span}""",
    )

    # Deflate to 2021 BRL, as the paper does. The deflator is a cumulative IPCA
    # index rebased on 2021, so a payment made in 2014 is inflated up and one made
    # in 2026 is discounted back. Without this the totals are not comparable --
    # nominal understates the published figure by ~18% over 2014-2021 alone.
    ipca = {
        r["ano"]: r["v"]
        for r in cli.query(
            "SELECT ano, (EXP(SUM(LN(1 + variacao_mensal/100))) - 1) * 100 AS v "
            "FROM `basedosdados.br_ibge_ipca.mes_brasil` "
            "WHERE ano BETWEEN 1995 AND 2026 AND variacao_mensal IS NOT NULL "
            "GROUP BY ano"
        ).result()
    }
    index, running = {}, 1.0
    for year in sorted(ipca):
        running *= 1 + ipca[year] / 100
        index[year] = running
    per_year = {
        r["ano"]: r["bn"]
        for r in cli.query(
            f"SELECT ano, SUM(valor_final)/1e9 AS bn FROM {base}.pagamento "
            f"WHERE sigla_uf='{uf}' {span} GROUP BY ano"
        ).result()
    }
    deflated = sum(
        bn * (index[2021] / index[y])
        for y, bn in per_year.items()
        if bn is not None and y in index
    )

    got = {
        "n_emp": emp["obs"],
        "pct_positive": 100 * emp["positives"] / emp["obs"]
        if emp["obs"]
        else 0,
        "pct_procure": 100 * cov["procurement"] / cov["distinct_commitments"]
        if cov["distinct_commitments"]
        else 0,
        "pct_with_liq": 100 * cov["with_liq"] / cov["distinct_commitments"]
        if cov["distinct_commitments"]
        else 0,
        "pct_with_pag": 100 * cov["with_pag"] / cov["distinct_commitments"]
        if cov["distinct_commitments"]
        else 0,
        "n_liq": liq["distinct_verifications"],
        "n_pag": pag["distinct_payments"],
        "n_sellers": pag["sellers"],
        "total_payment_bn": deflated,
    }

    print(
        f"\n{uf}  {args.project}.{args.dataset}  "
        f"exercises {emp['y0']}-{emp['y1']}  municipalities {emp['municipios']}\n"
    )
    label = {
        "n_emp": "Commitments",
        "pct_positive": "Positive (%)",
        "pct_procure": "Procurement-related (%)",
        "pct_with_liq": "With verification (%)",
        "pct_with_pag": "With payment (%)",
        "n_liq": "Verifications",
        "n_pag": "Payments",
        "n_sellers": "Distinct sellers",
        "total_payment_bn": "Total payments (BRL bn, 2021)",
    }
    print(
        "{:<34}{:>18}{:>18}{:>12}".format(
            "metric", "computed", "paper (2014-21)", "delta %"
        )
    )
    for key, name in label.items():
        g, p = got[key], PAPER[key]
        d = 100 * (g - p) / p if p else 0
        fmt = "{:>18,.1f}" if isinstance(g, float) else "{:>18,}"
        print(("{:<34}" + fmt + "{:>18,.1f}{:>+11.1f}%").format(name, g, p, d))
    print(
        f"\nnominal total R${pag['nominal_bn']:.1f}bn -> "
        f"R${deflated:.1f}bn in 2021 BRL (IPCA, br_ibge_ipca.mes_brasil)"
    )


if __name__ == "__main__":
    main()
