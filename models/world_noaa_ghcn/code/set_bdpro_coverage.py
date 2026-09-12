"""Put the BD Pro rolling window on `observation`.

    python models/world_noaa_ghcn/code/set_bdpro_coverage.py --env staging
    python models/world_noaa_ghcn/code/set_bdpro_coverage.py --env prod

Data Basis paywalls the most recent window of any table refreshing monthly or
more often. `observation` refreshes weekly, so it carries a 6-month pro window;
everything older stays free.

This is a **prerequisite**, not the mechanism. `register_table_materialization_task`
recomputes the window and re-issues the BigQuery Row Access Policies on every
pipeline run, but `assert_coverage_topology` raises before writing anything
unless BOTH a free and a pro Coverage already exist on the table. This script
creates the pro Coverage and seeds both DateTimeRanges.

Two details that are easy to get wrong:

* `is_closed` is the free/pro discriminator and its polarity is counterintuitive
  — free is `is_closed=False`, pro is `is_closed=True`. It must be set on the
  Coverage *and* on its DateTimeRange; they are separate records.
* The pipeline never writes `is_closed` on a range (`DateTimeRangeInput` has no
  such field), so whatever is registered here is what stays.

Run against staging first, then prod. The prod run is what makes the paywall
real on the next armed pipeline run.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

_MCP_PATH = os.environ.get(
    "BD_MCP_PATH",
    str(Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"),
)
if not Path(_MCP_PATH).is_dir():
    raise SystemExit(
        f"databasis MCP checkout not found at {_MCP_PATH!r}. Set BD_MCP_PATH."
    )
sys.path.insert(0, _MCP_PATH)

import server  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.world_noaa_ghcn.flows import _COVERAGE  # noqa: E402
from pipelines.utils.metadata.policy import (  # noqa: E402
    CoverageIds,
    assert_coverage_topology,
    compute_coverage_ranges,
)

DATASET_SLUG = "ghcn_daily"
TABLE = "observation"
AREA_SLUG = "world"
FIRST_YEAR = 1763


def _range_fields(dump: dict, position: str) -> dict[str, int]:
    """Pull one end of a computed range out of the DTO's camelCase keys."""
    return {
        "year": dump[f"{position}Year"],
        "month": dump[f"{position}Month"],
        "day": dump[f"{position}Day"],
    }


def main() -> None:
    """Create the pro Coverage and seed both DateTimeRanges."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging", choices=["staging", "prod"])
    ap.add_argument(
        "--source-end",
        required=True,
        help="Max date in the table, YYYY-MM-DD. Read it from BigQuery.",
    )
    args = ap.parse_args()
    env = args.env
    source_end = date.fromisoformat(args.source_end)

    area_id = server.lookup_id(category="area", slug=AREA_SLUG, env=env)["id"]
    dataset = server.get_dataset(DATASET_SLUG, env=env)
    table = dataset["tables"][TABLE]
    table_id = table["id"]

    # Classify what is already there. is_closed lives on the Coverage record and
    # get_dataset does not return it, so read it back over GraphQL.
    free_id = pro_id = None
    for cov in table.get("coverages") or []:
        q = "query($id: ID!) { allCoverage(id: $id) { edges { node { isClosed } } } }"
        edges = server._gql(q, {"id": cov["id"]}, env=env)["allCoverage"][
            "edges"
        ]
        closed = bool(edges and edges[0]["node"]["isClosed"])
        if closed:
            pro_id = cov["id"]
        else:
            free_id = cov["id"]
    print(f"existing coverages -> free={free_id} pro={pro_id}")

    if free_id is None:
        raise SystemExit(
            "no free Coverage found; run register_metadata.py first"
        )
    if pro_id is None:
        pro = server.create_update_coverage(
            table_id=table_id, area_id=area_id, is_closed=True, env=env
        )
        pro_id = pro["id"] if isinstance(pro, dict) else pro
        print(f"created pro Coverage {pro_id}")

    spec = _COVERAGE[TABLE]
    ids = CoverageIds(free=free_id, pro=pro_id)
    assert_coverage_topology(spec, ids)
    ranges = compute_coverage_ranges(spec, source_end, ids)

    free_end = _range_fields(ranges.free.model_dump(), "end")
    pro_start = _range_fields(ranges.pro.model_dump(), "start")
    pro_end = _range_fields(ranges.pro.model_dump(), "end")

    existing = {
        c["id"]: (c.get("datetime_ranges") or [])
        for c in (table.get("coverages") or [])
    }
    free_range_id = (existing.get(free_id) or [{}])[0].get("id")

    server.create_update_datetime_range(
        id=free_range_id,
        coverage_id=free_id,
        start_year=FIRST_YEAR,
        start_month=1,
        start_day=1,
        end_year=free_end["year"],
        end_month=free_end["month"],
        end_day=free_end["day"],
        interval=1,
        is_closed=False,
        env=env,
    )
    server.create_update_datetime_range(
        id=(existing.get(pro_id) or [{}])[0].get("id"),
        coverage_id=pro_id,
        start_year=pro_start["year"],
        start_month=pro_start["month"],
        start_day=pro_start["day"],
        end_year=pro_end["year"],
        end_month=pro_end["month"],
        end_day=pro_end["day"],
        interval=1,
        is_closed=True,
        env=env,
    )
    print(
        f"free  {FIRST_YEAR}-01-01 .. "
        f"{free_end['year']}-{free_end['month']:02d}-{free_end['day']:02d}  (is_closed=False)\n"
        f"pro   {pro_start['year']}-{pro_start['month']:02d}-{pro_start['day']:02d} .. "
        f"{pro_end['year']}-{pro_end['month']:02d}-{pro_end['day']:02d}  (is_closed=True)"
    )
    print("done. The pipeline rolls this window forward on every run.")


if __name__ == "__main__":
    main()
