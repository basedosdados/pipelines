"""Link each table's identifying columns to its observation level.

    uv run python models/us_dot_bts_ontime/code/link_observation_levels.py staging

Without this the site renders the level's columns as "Não informado".
`bulk_upsert_columns` does not set the FK, so it has to be a per-column
`update_column` call.

`update_column`'s boolean arguments default to False, so a bare call CLOBBERS
`is_partition`. `year` is the partition column and therefore re-passes the flag in
the same call.
"""

from __future__ import annotations

import sys

import databasis_mcp.tools.metadata as server
import databasis_mcp.tools.write as write

FLIGHT = "245b6498-5295-44df-8f4d-62496f2ba898"
AIRPORT = "2a0d9769-c080-4a84-a77e-016996a3fae8"

OL_FLIGHT = "5b869f8d-4369-4b41-9a41-08081b17cf51"
OL_YEAR = "bb44a10c-1f01-49ee-b850-bbd0181e28fc"
OL_AIRPORT = "6c322fad-39ad-4ad0-9ccf-f048cd68f4f1"

# (table_id, column_name, observation_level_id, is_partition)
LINKS = [
    # The flight grain: date + carrier + flight number + origin identifies a row.
    (FLIGHT, "flight_date", OL_FLIGHT, False),
    (FLIGHT, "reporting_carrier", OL_FLIGHT, False),
    (FLIGHT, "flight_number", OL_FLIGHT, False),
    (FLIGHT, "origin", OL_FLIGHT, False),
    (FLIGHT, "year", OL_YEAR, True),
    (AIRPORT, "airport_id", OL_AIRPORT, False),
]


def call(tool):
    return getattr(tool, "fn", tool)


def column_ids(env: str, table_id: str) -> dict[str, str]:
    """Column name -> id, read straight from the backend for one table."""
    query = """
    query($id: ID!) {
      allColumn(table_Id: $id, first: 1000) {
        edges { node { id name } }
      }
    }
    """
    data = server._gql(query, {"id": table_id}, env=env)
    out = {}
    for e in data["allColumn"]["edges"]:
        n = e["node"]
        out[n["name"]] = n["id"].split(":")[-1]
    return out


def main(env: str) -> None:
    # pyrefly: ignore [not-callable]
    call(server.auth)(env=env)
    cache: dict[str, dict[str, str]] = {}
    for table_id, name, ol_id, is_partition in LINKS:
        if table_id not in cache:
            cache[table_id] = column_ids(env, table_id)
        cid = cache[table_id].get(name)
        if cid is None:
            raise SystemExit(f"column {name} not found on table {table_id}")
        # pyrefly: ignore [not-callable]
        call(write.update_column)(
            column_id=cid,
            column_name=name,
            table_id=table_id,
            observation_level_id=ol_id,
            is_partition=is_partition,
            env=env,
        )
        print(f"{name:<22} -> OL {ol_id[:8]}  is_partition={is_partition}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "staging")
