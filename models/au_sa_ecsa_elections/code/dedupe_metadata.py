"""Delete duplicate child records left by a second registration pass.

``create_update_cloud_table``, ``create_update_observation_level``,
``create_update_datetime_range`` and ``create_update_update`` all create a new
record when no id is supplied — they do not match on their natural key. A second
registration run therefore doubles every one of them silently.

This keeps the oldest record of each natural key and deletes the rest. It is safe
to re-run: with no duplicates left it deletes nothing.

Usage::

    PYTHONPATH=. python models/au_sa_ecsa_elections/code/dedupe_metadata.py [--apply]
"""

from __future__ import annotations

import pathlib
import sys

MCP = (
    pathlib.Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
sys.path.insert(0, str(MCP))

import server  # noqa: E402

ENV = "staging"
DATASET_ID = "b2e707f6-b081-4cf8-a9d6-0435f13591f2"

QUERY = (
    f'{{ allDataset(id: "{DATASET_ID}") {{ edges {{ node {{ '
    "tables { edges { node { slug "
    "cloudTables { edges { node { id gcpProjectId gcpDatasetId gcpTableId } } } "
    "coverages { edges { node { id "
    "datetimeRanges { edges { node { id startYear endYear } } } } } } "
    "observationLevels { edges { node { id entity { slug } } } } "
    "updates { edges { node { id entity { slug } frequency } } } "
    "} } } } } } }"
)

DELETE = {
    "cloud table": "DeleteCloudTable",
    "datetime range": "DeleteDateTimeRange",
    "observation level": "DeleteObservationLevel",
    "update": "DeleteUpdate",
}


def delete(kind: str, record_id: str, apply: bool) -> None:
    if not apply:
        return
    name = DELETE[kind]
    mutation = f"mutation($id: UUID!) {{ {name}(id: $id) {{ ok errors }} }}"
    server._gql(mutation, {"id": record_id}, env=ENV, auth=True)


def surplus(nodes: list[dict], key) -> list[dict]:
    """Every node after the first for each natural key, in stable order."""
    seen: set = set()
    extra = []
    for node in nodes:
        signature = key(node)
        if signature in seen:
            extra.append(node)
        else:
            seen.add(signature)
    return extra


def main(argv: list[str]) -> int:
    apply = "--apply" in argv
    node = server._gql(QUERY, {}, env=ENV)["allDataset"]["edges"][0]["node"]
    removed = 0
    for edge in node["tables"]["edges"]:
        table = edge["node"]
        groups = [
            (
                "cloud table",
                [e["node"] for e in table["cloudTables"]["edges"]],
                lambda n: (
                    n["gcpProjectId"],
                    n["gcpDatasetId"],
                    n["gcpTableId"],
                ),
            ),
            (
                "observation level",
                [e["node"] for e in table["observationLevels"]["edges"]],
                lambda n: n["entity"]["slug"],
            ),
            (
                "update",
                [e["node"] for e in table["updates"]["edges"]],
                lambda n: (n["entity"]["slug"], n["frequency"]),
            ),
        ]
        for coverage in table["coverages"]["edges"]:
            groups.append(
                (
                    "datetime range",
                    [
                        e["node"]
                        for e in coverage["node"]["datetimeRanges"]["edges"]
                    ],
                    lambda n: (n["startYear"], n["endYear"]),
                )
            )
        for kind, nodes, key in groups:
            for extra in surplus(nodes, key):
                record_id = server._strip_id(extra["id"])
                print(
                    f"  {'deleting' if apply else 'would delete'} {kind} {record_id} on {table['slug']}"
                )
                delete(kind, record_id, apply)
                removed += 1
    print(
        f"{'deleted' if apply else 'would delete'} {removed} duplicate records"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
