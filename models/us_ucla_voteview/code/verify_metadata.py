"""Read us_ucla_voteview metadata back from a backend and check it against the architecture.

Usage (from the repo root):
    PYTHONPATH=. ~/.venvs/databasis-mcp/bin/python \
        models/us_ucla_voteview/code/verify_metadata.py --env staging|prod

Exits non-zero on any mismatch. get_dataset does not return column types,
coverage tiers or directory links, so everything is read over GraphQL.
"""

import argparse
import csv
import sys
from pathlib import Path

import databasis_mcp.tools.metadata as bd_meta

ARCH = Path(__file__).resolve().parent / "architecture"
sys.path.insert(0, str(ARCH.parent))
from register_metadata import DATASET_SLUG, PARTITIONS, TABLES  # noqa: E402

Q = """
query($id: ID) { allTable(id: $id) { edges { node { slug status { slug }
  dataset { status { slug } tags { edges { node { slug } } } organizations { edges { node { slug } } } }
  rawDataSource { edges { node { id } } }
  cloudTables { edges { node { gcpProjectId gcpDatasetId gcpTableId } } }
  updates { edges { node { latest entity { slug } } } }
  observationLevels { edges { node { id entity { slug } } } }
  coverages { edges { node { isClosed area { slug }
    datetimeRanges { edges { node { startYear endYear isClosed } } } } } }
  columns { edges { node { name order isPartition bigqueryType { name } measurementUnit
    coveredByDictionary descriptionPt descriptionEs
    observationLevel { id } directoryPrimaryKey { name table { slug dataset { slug } } } } } }
} } } }
"""


def nodes(n, key):
    return [e["node"] for e in n[key]["edges"]]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", required=True, choices=["dev", "staging", "prod"])
    env = ap.parse_args().env
    ds = bd_meta.get_dataset(slug=DATASET_SLUG, env=env)
    problems = []
    for table, spec in TABLES.items():
        tid = ds["tables"][table]["id"]
        n = bd_meta._gql(Q, {"id": tid}, env=env)["allTable"]["edges"][0][
            "node"
        ]
        with open(ARCH / f"{table}.csv") as f:
            arch = list(csv.DictReader(f))
        cols = sorted(nodes(n, "columns"), key=lambda c: c["order"] or 0)
        if [c["name"] for c in cols] != [a["name"] for a in arch]:
            problems.append(f"{table}: column names/order differ")
        ols = {
            bd_meta._strip_id(o["id"]): o["entity"]["slug"]
            for o in nodes(n, "observationLevels")
        }
        if sorted(ols.values()) != sorted(spec["levels"]):
            problems.append(
                f"{table}: OLs {sorted(ols.values())} != {sorted(spec['levels'])}"
            )
        level_of = {c: e for e, c in spec["levels"].items()}
        dir_links = 0
        for c, a in zip(
            cols, arch, strict=False
        ):  # a length mismatch is reported above
            name = a["name"]
            if (c["bigqueryType"] or {}).get("name") != a["bigquery_type"]:
                problems.append(
                    f"{table}.{name}: type {c['bigqueryType']} != {a['bigquery_type']}"
                )
            if c["isPartition"] != (name in PARTITIONS.get(table, [])):
                problems.append(
                    f"{table}.{name}: isPartition={c['isPartition']}"
                )
            if c["coveredByDictionary"] != (
                a["covered_by_dictionary"] == "yes"
            ):
                problems.append(
                    f"{table}.{name}: coveredByDictionary={c['coveredByDictionary']}"
                )
            if (c["measurementUnit"] or "") != a["measurement_unit"]:
                problems.append(
                    f"{table}.{name}: unit {c['measurementUnit']!r}"
                )
            if not c["descriptionPt"] or not c["descriptionEs"]:
                problems.append(f"{table}.{name}: missing PT/ES description")
            ol = c["observationLevel"]
            got = ols.get(bd_meta._strip_id(ol["id"])) if ol else None
            if got != level_of.get(name):
                problems.append(
                    f"{table}.{name}: OL link {got} != {level_of.get(name)}"
                )
            if a["directory_column"]:
                dpk = c["directoryPrimaryKey"]
                if dpk:
                    dir_links += 1
                    target = f"{dpk['table']['dataset']['slug']}.{dpk['table']['slug']}:{dpk['name']}"
                    print(f"  {table}.{name} -> {target}")
                else:
                    print(
                        f"  {table}.{name}: NO directory link (wanted {a['directory_column']})"
                    )
        covs = nodes(n, "coverages")
        tiers = sorted("pro" if c["isClosed"] else "free" for c in covs)
        want = {"free": ["free"], "part_bdpro": ["free", "pro"], None: []}[
            spec["tier"]
        ]
        if tiers != want:
            problems.append(f"{table}: coverage tiers {tiers} != {want}")
        ranges = {
            ("pro" if c["isClosed"] else "free"): nodes(c, "datetimeRanges")
            for c in covs
        }
        if spec["tier"] == "part_bdpro" and any(ranges.values()):
            problems.append(
                f"{table}: part_bdpro table has a static range {ranges}"
            )
        if len(nodes(n, "cloudTables")) != 1 or len(nodes(n, "updates")) != 1:
            problems.append(
                f"{table}: {len(nodes(n, 'cloudTables'))} cloud tables, {len(nodes(n, 'updates'))} updates"
            )
        if len(nodes(n, "rawDataSource")) != 1:
            problems.append(
                f"{table}: {len(nodes(n, 'rawDataSource'))} raw sources"
            )
        print(
            f"{table}: {len(cols)} cols, OLs {sorted(ols.values())}, coverages {tiers} "
            f"ranges { {k: [(r['startYear'], r['endYear']) for r in v] for k, v in ranges.items()} }, "
            f"cloud {nodes(n, 'cloudTables')[0]['gcpProjectId'] if nodes(n, 'cloudTables') else None}, "
            f"dir links {dir_links}"
        )
    d = n["dataset"]
    print(
        f"dataset status={d['status']['slug']} orgs={[o['slug'] for o in nodes(d, 'organizations')]} "
        f"tags={[t['slug'] for t in nodes(d, 'tags')]}"
    )
    if len(nodes(d, "tags")) != 6:
        problems.append(f"dataset has {len(nodes(d, 'tags'))} tags, want 6")
    print("\n".join(problems) if problems else "OK: no mismatches")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
