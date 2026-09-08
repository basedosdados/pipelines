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
# One entry per (table, area). The second element is a LIST of ranges, because a
# state's series is not always continuous: Rio Grande do Sul is missing six months
# that its own catalogue does not publish, and a single 2012-2026 range would promise
# data that is not there.
#
# Multiple RANGES rather than multiple coverages: a Coverage is keyed by area, so
# duplicating it for one state produces ambiguous records the backend has no tool to
# delete, and duplicates break CreateUpdateTable later. DateTimeRange is the repeating
# child meant for exactly this.
#
# `None` in place of the list means the table carries no date column at all.
PLAN: dict[
    str, list[tuple[str, list[tuple[int, int | None, int, int | None]] | None]]
] = {
    "despesa": [
        ("br_mg", [(2002, 1, 2026, 8)]),
        ("br_pe", [(2008, None, 2026, None)]),
        # Measured 2026-09-08. ES carries a full `Data` on every expense row, so
        # unlike PE it is genuinely month-granular across the whole series.
        ("br_es", [(2009, 1, 2026, 9)]),
        # Seven spans, derived from the data on 2026-09-08 and summing to exactly the
        # 169 months present. The gaps are 2020-06, 2020-08, 2022-04, 2023-02, 2023-06
        # and 2023-08: RS's catalogue serves the NEIGHBOURING month's file in those
        # slots, so the months were never published rather than lost in transit.
        (
            "br_rs",
            [
                (2012, 1, 2020, 5),
                (2020, 7, 2020, 7),
                (2020, 9, 2022, 3),
                (2022, 5, 2023, 1),
                (2023, 3, 2023, 5),
                (2023, 7, 2023, 7),
                (2023, 9, 2026, 7),
            ],
        ),
    ],
    "pagamento": [("br_pe", [(2008, 1, 2026, 8)])],
    "despesa_mensal": [("br_ba", [(2013, 1, 2026, 8)])],
    "despesa_anual": [("br_sp", [(2010, None, 2026, None)])],
    "empenho_credor": [("br_ba", [(2019, 1, 2026, 8)])],
    "licitacao": [
        ("br_ba", [(2004, 1, 2026, 12)]),
        ("br_mg", [(2009, 1, 2024, 3)]),
        ("br_es", [(2009, 8, 2026, 8)]),
    ],
    "licitacao_item": [
        ("br_ba", [(2004, None, 2026, None)]),
        ("br_mg", [(2009, None, 2025, None)]),
        # Year-granular: the canonical licitacao_item has no `mes`.
        ("br_es", [(2009, None, 2026, None)]),
    ],
    "licitacao_participante": [
        ("br_ba", [(2004, None, 2026, None)]),
        ("br_es", [(2009, None, 2026, None)]),
    ],
    "relacionamentos": [
        ("br_ba", None),
        ("br_mg", None),
        ("br_es", None),
    ],
    "dicionario": [
        ("br_mg", None),
        ("br_es", None),
    ],
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
        for area, ranges in entries:
            uf = area.removeprefix("br_").upper()
            if ranges is None:
                continue
            y0, y1 = ranges[0][0], ranges[-1][2]
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
        # Matched BY AREA, never by position. The backend does not return coverages in
        # PLAN order -- on `licitacao` it has br_mg before br_ba where PLAN has BA
        # first -- so an index-based pairing writes each state's range onto another
        # state's coverage record. The end state happened to come out right only
        # because every entry was rewritten in the same pass; a PLAN shorter than the
        # backend's list, or a failure midway, would leave ranges on the wrong states.
        existing = {c["area_slug"]: c for c in table["coverages"]}
        for area, ranges in entries:
            # Reuse the coverage already on the table where there is one: the backend
            # has no delete-coverage tool, and duplicates break CreateUpdateTable later.
            kwargs = {
                "table_id": table["id"],
                "area_id": areas[area],
                "env": env,
            }
            prior_cov = existing.get(area)
            if prior_cov:
                kwargs["id"] = prior_cov["id"]
            coverage = server.create_update_coverage(**kwargs)
            if ranges is None:
                print(f"  {slug:24} {area}  no range")
                continue

            prior = (prior_cov or {}).get("datetime_ranges") or []
            if len(prior) > len(ranges):
                # There is no delete tool for a DateTimeRange either, so a shrinking
                # list would leave stale spans claiming coverage that was withdrawn.
                # Refuse rather than half-apply.
                raise SystemExit(
                    f"{slug}/{area}: {len(prior)} range(s) registered but only "
                    f"{len(ranges)} planned; the extra ones cannot be deleted here"
                )
            for i, (y0, m0, y1, m1) in enumerate(ranges):
                rng = {
                    "coverage_id": coverage["id"],
                    "start_year": y0,
                    "end_year": y1,
                    "interval": 1,
                    "env": env,
                }
                if m0:
                    rng.update(start_month=m0, end_month=m1)
                if i < len(prior):
                    rng["id"] = prior[i]["id"]
                server.create_update_datetime_range(**rng)
            label = ", ".join(
                f"{y0}-{m0:02d}..{y1}-{m1:02d}" if m0 else f"{y0}..{y1}"
                for y0, m0, y1, m1 in ranges
            )
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
