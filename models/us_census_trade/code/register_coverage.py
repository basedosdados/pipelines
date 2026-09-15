"""Register us_census_trade coverage, datetime ranges and Update records.

Run this AFTER the first dev flow run, not before: the maximum month has to be
read from the data that actually landed, and a month-granular table registered
with year-only bounds renders wrong on the site and silently degrades the
source poll to an annual comparison.

Each fact table gets TWO coverages, because all six refresh monthly and
therefore carry the BD Pro rolling window under the house rule:

    free coverage  is_closed=False   2010-01 .. (max month - 6 months)
    pro  coverage  is_closed=True    (free_end + 1 month) .. max month

Both the Coverage and its DateTimeRange carry is_closed, and the two ranges must
not overlap -- free_end is inclusive, so pro starts the following month. Both
coverages must exist before the pipeline's first armed run, or
``assert_coverage_topology`` hard-fails with "part_bdpro exige Coverage free +
pro". The pipeline rewrites the ranges on every run from there.

``dicionario`` has no date column and takes no coverage at all.

Usage:
    ~/.venvs/bd-pipelines/bin/python models/us_census_trade/code/register_coverage.py \
        --max-month 2026-06 [--env staging]

Read --max-month from the landed data, for example:
    select max(year * 100 + month) from `basedosdados-dev.us_census_trade.import`
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
import server

DATASET_SLUG = "foreign_trade"
AREA_SLUG = "us"
FIRST_YEAR, FIRST_MONTH = 2010, 1
FREE_LAG_MONTHS = 6

FACT_TABLES = [
    "import",
    "export",
    "import_port",
    "export_port",
    "import_state",
    "export_state",
]

# The table whose raw data source carries the source Update. Only one table may
# be linked to a raw source for the poll to resolve, and this dataset links one
# shared source to all six, so the poll table is named explicitly.
POLL_TABLE = "import"


def bare_id(v: str) -> str:
    return v.split(":")[-1] if v else v


def minus_months(year: int, month: int, n: int) -> tuple[int, int]:
    idx = year * 12 + (month - 1) - n
    return idx // 12, idx % 12 + 1


def plus_months(year: int, month: int, n: int) -> tuple[int, int]:
    idx = year * 12 + (month - 1) + n
    return idx // 12, idx % 12 + 1


def main(env: str, max_month: str) -> None:
    max_year, max_mon = (int(x) for x in max_month.split("-"))
    free_y, free_m = minus_months(max_year, max_mon, FREE_LAG_MONTHS)
    pro_y, pro_m = plus_months(free_y, free_m, 1)
    print(f"max month {max_year:04d}-{max_mon:02d}")
    print(
        f"  free {FIRST_YEAR:04d}-{FIRST_MONTH:02d} .. {free_y:04d}-{free_m:02d}"
    )
    print(f"  pro  {pro_y:04d}-{pro_m:02d} .. {max_year:04d}-{max_mon:02d}")
    if (free_y, free_m) >= (pro_y, pro_m):
        raise SystemExit("free and pro ranges overlap")

    area_id = server.lookup_id(category="area", slug=AREA_SLUG, env=env)["id"]
    entity_month = server.lookup_id(category="entity", slug="month", env=env)[
        "id"
    ]
    ds = server.get_dataset(slug=DATASET_SLUG, env=env)
    if not ds["found"]:
        raise SystemExit(f"dataset {DATASET_SLUG} not registered in {env}")

    today = dt.date.today().isoformat()

    for table in FACT_TABLES:
        info = ds["tables"][table]
        table_id = bare_id(info["id"])
        existing = info.get("coverages", [])
        # Reuse by is_closed so a re-run updates rather than duplicating.
        by_closed = {bool(c.get("is_closed")): c for c in existing}

        for is_closed, (sy, sm, ey, em) in (
            (False, (FIRST_YEAR, FIRST_MONTH, free_y, free_m)),
            (True, (pro_y, pro_m, max_year, max_mon)),
        ):
            prev = by_closed.get(is_closed)
            cov = server.create_update_coverage(
                table_id=table_id,
                area_id=area_id,
                is_closed=is_closed,
                id=bare_id(prev["id"]) if prev else None,
                env=env,
            )
            cov_id = bare_id(cov["id"])
            prev_range = (prev or {}).get("datetime_ranges") or []
            server.create_update_datetime_range(
                coverage_id=cov_id,
                start_year=sy,
                start_month=sm,
                end_year=ey,
                end_month=em,
                interval=1,
                # Matches its Coverage: the pro range is closed data.
                is_closed=is_closed,
                id=bare_id(prev_range[0]["id"]) if prev_range else None,
                env=env,
            )
            label = "pro " if is_closed else "free"
            print(f"  {table:14s} {label} {sy}-{sm:02d}..{ey}-{em:02d}")

        # Table Update: when WE last refreshed the table -- a wall clock.
        prev_upd = info.get("updates") or []
        server.create_update_update(
            table_id=table_id,
            entity_id=entity_month,
            frequency=1,
            lag=1,  # month M is published in M+1
            latest=today,
            id=bare_id(prev_upd[0]["id"]) if prev_upd else None,
            env=env,
        )

    # Source Update: what the SOURCE published -- its max COVERAGE date, not
    # today. Created here rather than waiting for the first run with
    # update_metadata=True, which would otherwise leave a Poll and no Update.
    sources = server.get_raw_data_sources(dataset_slug=DATASET_SLUG, env=env)
    if sources:
        server.create_update_update(
            raw_data_source_id=bare_id(sources[0]["id"]),
            entity_id=entity_month,
            frequency=1,
            latest=f"{max_year:04d}-{max_mon:02d}-01",
            env=env,
        )
        print(
            f"\nsource Update latest={max_year:04d}-{max_mon:02d}-01 (coverage date)"
        )
    print(f"table Update latest={today} (wall clock)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    ap.add_argument(
        "--max-month", required=True, help="YYYY-MM from the landed data"
    )
    a = ap.parse_args()
    main(a.env, a.max_month)
