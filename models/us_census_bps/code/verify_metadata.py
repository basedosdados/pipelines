"""Read the registered us_census_bps metadata back and check it is complete.

Every write in this onboarding is verified by reading it back, not by trusting
the mutation's return value.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
sys.path.insert(0, str(Path(__file__).resolve().parent))

import server
from metadata import DATASET_SLUG, TABLE_ORDER

ARCH = Path(__file__).resolve().parent / "architecture"

QUERY = """
{
  allDataset(slug: "%s") {
    edges { node {
      slug status { slug }
      themes { edges { node { slug } } }
      tags { edges { node { slug } } }
      organizations { edges { node { slug } } }
      tables(first: 20) { edges { node {
        slug order auxiliaryFilesUrl
        status { slug }
        rawDataSource { edges { node { url } } }
        cloudTables { edges { node { gcpProjectId gcpDatasetId gcpTableId } } }
        observationLevels { edges { node {
          entity { slug }
          columns { edges { node { name } } }
        } } }
        coverages { edges { node {
          isClosed
          datetimeRanges { edges { node {
            startYear startMonth endYear endMonth isClosed
          } } }
        } } }
        updates { edges { node { entity { slug } frequency lag latest } } }
        columns(first: 60) { edges { node {
          name isPartition coveredByDictionary measurementUnit
          bigqueryType { name }
          directoryPrimaryKey { name table { slug dataset { slug } } }
          descriptionEn descriptionPt
        } } }
      } } }
    } }
  }
}
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="staging")
    args = parser.parse_args()

    node = server._gql(QUERY % DATASET_SLUG, {}, env=args.env)["allDataset"][
        "edges"
    ][0]["node"]
    problems: list[str] = []

    print(f"dataset {node['slug']} — status {node['status']['slug']}")
    print(
        f"  organizations {[o['node']['slug'] for o in node['organizations']['edges']]}"
    )
    print(f"  themes {[t['node']['slug'] for t in node['themes']['edges']]}")
    print(f"  tags {[t['node']['slug'] for t in node['tags']['edges']]}")
    if not node["tags"]["edges"]:
        problems.append("dataset has no tags")

    tables = {t["node"]["slug"]: t["node"] for t in node["tables"]["edges"]}
    if set(tables) != set(TABLE_ORDER):
        problems.append(f"table set mismatch: {sorted(set(tables))}")

    for slug in TABLE_ORDER:
        t = tables[slug]
        expected = [
            c["name"] for c in csv.DictReader((ARCH / f"{slug}.csv").open())
        ]
        got = {c["node"]["name"]: c["node"] for c in t["columns"]["edges"]}
        missing = [c for c in expected if c not in got]
        partitions = [n for n, c in got.items() if c["isPartition"]]
        fks = {
            n: f"{c['directoryPrimaryKey']['table']['dataset']['slug']}."
            f"{c['directoryPrimaryKey']['table']['slug']}:"
            f"{c['directoryPrimaryKey']['name']}"
            for n, c in got.items()
            if c["directoryPrimaryKey"]
        }
        dicts = sorted(n for n, c in got.items() if c["coveredByDictionary"])
        no_desc = sorted(
            n for n, c in got.items() if not (c["descriptionEn"] or "").strip()
        )
        ols = [
            (
                o["node"]["entity"]["slug"],
                [c["node"]["name"] for c in o["node"]["columns"]["edges"]],
            )
            for o in t["observationLevels"]["edges"]
        ]
        cov = [
            (
                c["node"]["isClosed"],
                [
                    (
                        r["node"]["startYear"],
                        r["node"]["startMonth"],
                        r["node"]["endYear"],
                        r["node"]["endMonth"],
                    )
                    for r in c["node"]["datetimeRanges"]["edges"]
                ],
            )
            for c in t["coverages"]["edges"]
        ]
        ct = [
            f"{c['node']['gcpProjectId']}.{c['node']['gcpDatasetId']}."
            f"{c['node']['gcpTableId']}"
            for c in t["cloudTables"]["edges"]
        ]
        sources = [s["node"]["url"] for s in t["rawDataSource"]["edges"]]
        upd = [
            (
                u["node"]["entity"]["slug"],
                u["node"]["frequency"],
                u["node"]["lag"],
                (u["node"]["latest"] or "")[:10],
            )
            for u in t["updates"]["edges"]
        ]

        print(f"\n{slug}  (order {t['order']}, status {t['status']['slug']})")
        print(f"  columns {len(got)}/{len(expected)}  partitions {partitions}")
        print(f"  observation levels {ols}")
        print(f"  directory links {fks}")
        print(f"  dictionary-covered {dicts}")
        print(f"  coverage {cov}")
        print(f"  update {upd}")
        print(f"  cloud table {ct}  raw sources {len(sources)}")
        print(
            f"  auxiliary files {'set' if t['auxiliaryFilesUrl'] else 'MISSING'}"
        )

        if missing:
            problems.append(f"{slug}: missing columns {missing}")
        if no_desc:
            problems.append(
                f"{slug}: columns without an English description {no_desc}"
            )
        if len(ct) != 1:
            problems.append(f"{slug}: {len(ct)} cloud tables")
        if slug != "dicionario":
            if partitions != ["year"]:
                problems.append(f"{slug}: partitions are {partitions}")
            if len(ols) != 2 or any(len(c) != 1 for _e, c in ols):
                problems.append(f"{slug}: observation levels {ols}")
            if not cov:
                problems.append(f"{slug}: no coverage")
            if len(upd) != 1:
                problems.append(f"{slug}: {len(upd)} update records")
            if len(sources) != 1:
                problems.append(f"{slug}: {len(sources)} raw sources linked")
            if not t["auxiliaryFilesUrl"]:
                problems.append(f"{slug}: no auxiliary files URL")

    print("\n" + "=" * 60)
    if problems:
        print(f"PROBLEMS ({len(problems)}):")
        for p in problems:
            print("  -", p)
        return 1
    print("All metadata checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
