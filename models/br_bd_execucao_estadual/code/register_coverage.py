"""Register one coverage per STATE on each br_bd_execucao_estadual table.

A single `br` coverage per table is wrong for this dataset. Every table is a union of
states that start and end in different years, so one national range silently claims
coverage a state does not have: `licitacao` as a whole spans 2004-2026, but that is Bahia
— Minas Gerais only runs 2009-03/2024. A reader filtering `sigla_uf = 'MG'` for 2025 gets
nothing, while the metadata promised data.

The ranges below are measured from the tables, not inherited from the table total. Rerun
`--check` after a refresh to see whether the data has outgrown them.

Month granularity is used only where the state actually publishes a usable month:

* Pernambuco is deliberately YEAR-level in `despesa`. It carries `mes` only for 2008-2010,
  and on those rows the field holds accounting periods rather than calendar months --
  28,517 rows use 13 (year-end close), and period 0 (opening) also occurs. A month-level
  range would be doubly wrong: absent for 16 of 19 exercises, and not months where present.
* Bahia's `licitacao` legitimately runs to 2026-12 because the state publishes tenders
  scheduled ahead of today.

Usage:
    uv run python models/br_bd_execucao_estadual/code/register_coverage.py [--check]
"""

from __future__ import annotations

import argparse
import sys
import warnings

warnings.filterwarnings("ignore")

MCP = "/Users/rdahis/Dropbox/BD/mcp"

# (area slug, start_year, start_month, end_year, end_month).
# A None month means the range is year-granular; None years mean no range at all, which is
# correct for the two tables that carry no date column.
PLAN: dict[
    str, list[tuple[str, int | None, int | None, int | None, int | None]]
] = {
    "despesa": [
        ("br_mg", 2002, 1, 2026, 8),
        ("br_pe", 2008, None, 2026, None),
    ],
    "pagamento": [("br_pe", 2008, 1, 2026, 8)],
    "despesa_mensal": [("br_ba", 2013, 1, 2026, 8)],
    "despesa_anual": [("br_sp", 2010, None, 2026, None)],
    "empenho_credor": [("br_ba", 2019, 1, 2026, 8)],
    "licitacao": [
        ("br_ba", 2004, 1, 2026, 12),
        ("br_mg", 2009, 1, 2024, 3),
    ],
    "licitacao_item": [
        ("br_ba", 2004, None, 2026, None),
        ("br_mg", 2009, None, 2025, None),
    ],
    "licitacao_participante": [("br_ba", 2004, None, 2026, None)],
    "relacionamentos": [
        ("br_ba", None, None, None, None),
        ("br_mg", None, None, None, None),
    ],
    "dicionario": [("br_mg", None, None, None, None)],
}

# Which column carries the month, where one is usable for a range.
MONTH_COLUMN = {
    "despesa": "mes",
    "pagamento": "mes",
    "despesa_mensal": "mes",
    "empenho_credor": "mes",
    "licitacao": "mes",
}


def measured(project: str) -> dict[tuple[str, str], tuple]:
    """What the data actually spans, per (table, state)."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    out = {}
    for slug, entries in PLAN.items():
        # relacionamentos and dicionario carry no date column at all, so there is no
        # range to outgrow and nothing to query.
        if all(e[1] is None for e in entries):
            continue
        month = MONTH_COLUMN.get(slug)
        extra = (
            f", min(ano * 100 + {month}) ym0, max(ano * 100 + {month}) ym1"
            if month
            else ""
        )
        try:
            rows = client.query(
                f"select sigla_uf, min(ano) y0, max(ano) y1{extra} "
                f"from `{project}.br_bd_execucao_estadual.{slug}` group by sigla_uf"
            ).result()
        except Exception as exc:  # table not built yet
            print(f"  {slug}: {type(exc).__name__}")
            continue
        for r in rows:
            out[(slug, r.sigla_uf)] = (
                r.y0,
                r.y1,
                getattr(r, "ym0", None),
                getattr(r, "ym1", None),
            )
    return out


def check(env: str) -> int:
    project = "basedosdados" if env == "prod" else "basedosdados-dev"
    seen = measured(project)
    stale = 0
    for slug, entries in PLAN.items():
        for area, y0, _, y1, _ in entries:
            uf = area.removeprefix("br_").upper()
            if y0 is None:
                continue
            got = seen.get((slug, uf))
            if got is None:
                print(f"  {slug} {uf}: no rows in the table")
                stale += 1
                continue
            if got[0] < y0 or got[1] > y1:
                stale += 1
                print(
                    f"  {slug} {uf}: data spans {got[0]}-{got[1]}, "
                    f"registered {y0}-{y1}  <-- OUTGROWN"
                )
    print(
        "coverage matches the data"
        if not stale
        else f"{stale} range(s) outgrown"
    )
    return stale


def apply(env: str) -> None:
    sys.path.insert(0, MCP)
    import server

    server.auth(env=env)
    areas = {
        a: server.lookup_id(category="area", slug=a, env=env)["id"]
        for a in {e[0] for v in PLAN.values() for e in v}
    }
    dataset = server.get_dataset(slug="execucao_estadual", env=env)
    for slug, entries in PLAN.items():
        table = dataset["tables"][slug]
        existing = table["coverages"]
        for i, (area, y0, m0, y1, m1) in enumerate(entries):
            # Reuse the coverage already on the table where there is one: the backend has
            # no delete-coverage tool, and duplicates break CreateUpdateTable later.
            kwargs = {
                "table_id": table["id"],
                "area_id": areas[area],
                "env": env,
            }
            if i < len(existing):
                kwargs["id"] = existing[i]["id"]
            coverage = server.create_update_coverage(**kwargs)
            if y0 is None:
                print(f"  {slug:24} {area}  no range")
                continue
            rng = {
                "coverage_id": coverage["id"],
                "start_year": y0,
                "end_year": y1,
                "interval": 1,
                "env": env,
            }
            if m0:
                rng.update(start_month=m0, end_month=m1)
            prior = (
                existing[i].get("datetime_ranges")
                if i < len(existing)
                else None
            )
            if prior:
                rng["id"] = prior[0]["id"]
            server.create_update_datetime_range(**rng)
            label = f"{y0}-{m0:02d}..{y1}-{m1:02d}" if m0 else f"{y0}..{y1}"
            print(f"  {slug:24} {area}  {label}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    ap.add_argument(
        "--check",
        action="store_true",
        help="compare the registered ranges against the data and exit",
    )
    args = ap.parse_args()
    if args.check:
        sys.exit(1 if check(args.env) else 0)
    apply(args.env)


if __name__ == "__main__":
    main()
