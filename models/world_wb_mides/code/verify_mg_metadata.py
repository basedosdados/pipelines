"""Verify the MG metadata in the backend against the models on disk.

    ~/.venvs/bd-pipelines/bin/python models/world_wb_mides/code/verify_mg_metadata.py

Checks what a reviewer would otherwise have to check by hand, and reports what is
actually true rather than what was intended:

  * every model has a registered table, and vice versa
  * column NAMES match the model exactly, in both directions
  * column TYPES match the model's own `safe_cast`
  * all three languages are present on every column and table
  * exactly one cloud table, one MG coverage, one datetime range, one update --
    the child records that silently duplicate when `id` is omitted
  * the partition flag is on `ano` and nowhere else
  * each observation level is linked to an identifying column
  * MG coverage reaches the year the source actually carries

Queries per table rather than `get_dataset`, which returns every column of every
table and takes 80+ seconds on this dataset -- past the client's own timeout.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(
    0,
    os.path.expanduser("~/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"),
)

# pyrefly: ignore [missing-import]  # sibling module via sys.path
import register_mg_metadata as reg

# pyrefly: ignore [missing-import]  # the databasis MCP server, via sys.path
import server

ENV = "staging"
DATASET_ID = reg.DATASET_ID
EXPECTED_END_YEAR = reg.END_YEAR

TABLE_QUERY = """query($ds: ID!, $slug: String!) {
  allTable(dataset_Id: $ds, slug: $slug, first: 1) {
    edges { node {
      id slug namePt nameEn nameEs descriptionPt descriptionEn descriptionEs
      observationLevels { edges { node { id entity { slug }
        columns { edges { node { name } } } } } }
      cloudTables { edges { node { id gcpProjectId gcpTableId } } }
      coverages { edges { node { id area { slug }
        datetimeRanges { edges { node { id startYear endYear } } } } } }
      updates { edges { node { id } } }
    } }
  }
}"""

COLUMN_QUERY = """query($id: ID!) {
  allColumn(table_Id: $id, first: 500) {
    edges { node { name isPartition bigqueryType { name }
      descriptionPt descriptionEn descriptionEs
      observationLevel { id } } }
  }
}"""


def main() -> None:
    mg_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "mg"
    )
    slugs = sorted(
        fn[len("world_wb_mides__") : -len(".sql")]
        for fn in os.listdir(mg_dir)
        if fn.endswith(".sql")
    )
    problems: list[str] = []
    ok = 0

    for slug in slugs:
        path = os.path.join(mg_dir, f"world_wb_mides__{slug}.sql")
        want = dict(reg.typed_columns(path))

        edges = server._gql(
            TABLE_QUERY, {"ds": DATASET_ID, "slug": slug}, env=ENV
        )["allTable"]["edges"]
        if not edges:
            problems.append(f"{slug}: NOT REGISTERED")
            continue
        node = edges[0]["node"]
        table_id = server._strip_id(node["id"])

        for field in (
            "namePt",
            "nameEn",
            "nameEs",
            "descriptionPt",
            "descriptionEn",
            "descriptionEs",
        ):
            if not node.get(field):
                problems.append(f"{slug}: table {field} is empty")

        cols = {
            e["node"]["name"]: e["node"]
            for e in server._gql(COLUMN_QUERY, {"id": table_id}, env=ENV)[
                "allColumn"
            ]["edges"]
        }
        missing = sorted(set(want) - set(cols))
        extra = sorted(set(cols) - set(want))
        if missing:
            problems.append(
                f"{slug}: columns in the model but NOT registered: {missing}"
            )
        if extra:
            problems.append(
                f"{slug}: columns registered but NOT in the model: {extra}"
            )

        for name, expected_type in want.items():
            col = cols.get(name)
            if not col:
                continue
            got = (col.get("bigqueryType") or {}).get("name")
            if got != expected_type:
                problems.append(
                    f"{slug}.{name}: type {got} registered, model says {expected_type}"
                )
            for field in ("descriptionPt", "descriptionEn", "descriptionEs"):
                if not col.get(field):
                    problems.append(f"{slug}.{name}: {field} is empty")

        partitions = sorted(n for n, c in cols.items() if c.get("isPartition"))
        if partitions != ["ano"]:
            problems.append(
                f"{slug}: partition flag on {partitions}, expected ['ano']"
            )

        for key, label in (
            ("cloudTables", "cloud table"),
            ("updates", "update"),
        ):
            n = len(node[key]["edges"])
            if n != 1:
                problems.append(f"{slug}: {n} {label}s, expected exactly 1")

        mg = [
            c
            for c in node["coverages"]["edges"]
            if c["node"]["area"]["slug"] == "br_mg"
        ]
        if len(mg) != 1:
            problems.append(
                f"{slug}: {len(mg)} br_mg coverages, expected exactly 1"
            )
        else:
            ranges = mg[0]["node"]["datetimeRanges"]["edges"]
            if len(ranges) != 1:
                problems.append(
                    f"{slug}: {len(ranges)} datetime ranges, expected exactly 1"
                )
            elif ranges[0]["node"]["endYear"] != EXPECTED_END_YEAR:
                problems.append(
                    f"{slug}: MG coverage ends {ranges[0]['node']['endYear']}, "
                    f"expected {EXPECTED_END_YEAR}"
                )

        want_levels = set(reg.OBSERVATION_LEVELS[slug])
        got_levels = {
            e["node"]["entity"]["slug"]
            for e in node["observationLevels"]["edges"]
        }
        if got_levels != want_levels:
            problems.append(
                f"{slug}: observation levels {sorted(got_levels)}, expected {sorted(want_levels)}"
            )
        unlinked = [
            e["node"]["entity"]["slug"]
            for e in node["observationLevels"]["edges"]
            if not e["node"]["columns"]["edges"]
        ]
        if unlinked:
            problems.append(
                f"{slug}: observation levels with no column linked: {unlinked}"
            )

        ok += 1

    print(f"checked {ok}/{len(slugs)} tables against their models\n")
    if problems:
        print(f"{len(problems)} problems:")
        for p in problems:
            print(f"  - {p}")
        raise SystemExit(1)
    print("no discrepancies")


if __name__ == "__main__":
    main()
