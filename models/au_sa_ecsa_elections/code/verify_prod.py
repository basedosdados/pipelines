"""Read the promoted metadata back from prod through raw GraphQL.

``get_dataset`` returns only {id, name, is_partition} per column, so it cannot
see the trilingual descriptions, the observations, the directory foreign keys or
the observation-level links. Everything below is enumerated per column and per
table rather than counted in aggregate.

Usage::

    python verify_prod.py
"""

from __future__ import annotations

import json
import pathlib
import sys

sys.path.insert(
    0,
    str(
        pathlib.Path.home()
        / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
    ),
)

import server

ENV = "prod"
SLUG = "sa_elections"
META = pathlib.Path(__file__).resolve().parent / "metadata"

TABLE_ORDER = [
    "election",
    "candidate",
    "result_district",
    "result_voting_centre",
    "distribution_of_preferences",
    "enrolment_turnout",
    "voting_centre",
    "disclosure_return",
    "dicionario",
]

DATASET_Q = """
{ allDataset(slug: "%s") { edges { node {
  id slug namePt nameEn nameEs descriptionPt descriptionEn descriptionEs
  status { slug }
  organizations { edges { node { id slug nameEn website } } }
  themes { edges { node { slug } } }
  tags { edges { node { slug } } }
  rawDataSources { edges { node { id url nameEn license { slug } availability { slug } } } }
  tables { edges { node { id slug } } }
} } } }
"""

TABLE_Q = """
{ allTable(id: "%s") { edges { node {
  id slug namePt nameEn nameEs status { slug }
  rawDataSource { edges { node { id url } } }
  cloudTables { edges { node { id gcpProjectId gcpDatasetId gcpTableId } } }
  observationLevels { edges { node { id entity { slug } } } }
  updates { edges { node { id entity { slug } frequency latest } } }
  coverages { edges { node { id isClosed area { slug }
    datetimeRanges { edges { node { id startYear endYear interval } } } } } }
} } } }
"""

COLUMN_Q = """
query($id: ID!) { allColumn(table_Id: $id) { edges { node {
  id name bigqueryType { name } isPartition isPrimaryKey
  descriptionPt descriptionEn descriptionEs
  observationsPt observationsEn observationsEs
  coveredByDictionary
  observationLevel { id entity { slug } }
  directoryPrimaryKey { id name table { slug dataset { slug } } }
} } } }
"""


def main() -> int:
    problems: list[str] = []
    node = server._gql(DATASET_Q % SLUG, {}, env=ENV)["allDataset"]["edges"][
        0
    ]["node"]
    dataset_id = server._strip_id(node["id"])
    print(f"dataset {SLUG} {dataset_id}  status={node['status']['slug']}")
    if node["status"]["slug"] != "under_review":
        problems.append(
            f"dataset status is {node['status']['slug']}, not under_review"
        )

    orgs = [
        (
            server._strip_id(o["node"]["id"]),
            o["node"]["slug"],
            o["node"]["website"],
        )
        for o in node["organizations"]["edges"]
    ]
    print(f"  organizations: {orgs}")
    print(f"  themes: {[t['node']['slug'] for t in node['themes']['edges']]}")
    tags = sorted(t["node"]["slug"] for t in node["tags"]["edges"])
    print(f"  tags ({len(tags)}): {tags}")

    expect_ds = json.loads((META / "dataset.json").read_text())
    for key, field in (
        ("description_pt", "descriptionPt"),
        ("description_en", "descriptionEn"),
        ("description_es", "descriptionEs"),
        ("name_pt", "namePt"),
        ("name_en", "nameEn"),
        ("name_es", "nameEs"),
    ):
        if node[field] != expect_ds[key]:
            problems.append(f"dataset {field} differs from the staging text")
    print(
        "  dataset name/description match staging verbatim:",
        all(
            node[f] == expect_ds[k]
            for k, f in (
                ("description_pt", "descriptionPt"),
                ("description_en", "descriptionEn"),
                ("description_es", "descriptionEs"),
                ("name_pt", "namePt"),
                ("name_en", "nameEn"),
                ("name_es", "nameEs"),
            )
        ),
    )

    print("  raw data sources:")
    source_url = {}
    for e in node["rawDataSources"]["edges"]:
        s = e["node"]
        sid = server._strip_id(s["id"])
        source_url[sid] = s["url"]
        lic = s["license"]["slug"] if s["license"] else None
        print(f"    {sid}  licence={lic}  {s['url']}")
        if lic != "unknown":
            problems.append(
                f"raw source {s['url']} carries licence {lic}, not unknown"
            )
    if len(node["rawDataSources"]["edges"]) != 2:
        problems.append(
            f"{len(node['rawDataSources']['edges'])} raw data sources, expected 2"
        )

    table_ids = {
        t["node"]["slug"]: server._strip_id(t["node"]["id"])
        for t in node["tables"]["edges"]
    }
    if sorted(table_ids) != sorted(TABLE_ORDER):
        problems.append(f"table set differs: {sorted(table_ids)}")

    total_columns = 0
    total_fk = 0
    total_partition = 0
    total_ol_links = 0
    print("\n=== tables ===")
    for slug in TABLE_ORDER:
        tid = table_ids.get(slug)
        if tid is None:
            problems.append(f"table {slug} missing")
            continue
        t = server._gql(TABLE_Q % tid, {}, env=ENV)["allTable"]["edges"][0][
            "node"
        ]

        srcs = [
            server._strip_id(e["node"]["id"])
            for e in t["rawDataSource"]["edges"]
        ]
        clouds = [
            (
                server._strip_id(c["node"]["id"]),
                f"{c['node']['gcpProjectId']}.{c['node']['gcpDatasetId']}.{c['node']['gcpTableId']}",
            )
            for c in t["cloudTables"]["edges"]
        ]
        ols = {
            server._strip_id(o["node"]["id"]): o["node"]["entity"]["slug"]
            for o in t["observationLevels"]["edges"]
        }
        ups = [
            (
                u["node"]["entity"]["slug"],
                u["node"]["frequency"],
                u["node"]["latest"],
            )
            for u in t["updates"]["edges"]
        ]
        covs = [
            (
                server._strip_id(c["node"]["id"]),
                c["node"]["isClosed"],
                c["node"]["area"]["slug"] if c["node"]["area"] else None,
                [
                    (r["node"]["startYear"], r["node"]["endYear"])
                    for r in c["node"]["datetimeRanges"]["edges"]
                ],
            )
            for c in t["coverages"]["edges"]
        ]

        cols = [
            e["node"]
            for e in server._gql(COLUMN_Q, {"id": tid}, env=ENV)["allColumn"][
                "edges"
            ]
        ]
        expect = {
            c["name"]: c
            for c in json.loads((META / f"{slug}.json").read_text())
        }

        pt_only = [
            c["name"]
            for c in cols
            if not (c["descriptionEn"] or "").strip()
            or not (c["descriptionEs"] or "").strip()
            or not (c["descriptionPt"] or "").strip()
        ]
        obs_partial = [
            c["name"]
            for c in cols
            if any(
                (c[f] or "").strip()
                for f in ("observationsPt", "observationsEn", "observationsEs")
            )
            and not all(
                (c[f] or "").strip()
                for f in ("observationsPt", "observationsEn", "observationsEs")
            )
        ]
        pks = [c["name"] for c in cols if c["isPrimaryKey"]]
        parts = [c["name"] for c in cols if c["isPartition"]]
        fks = {
            c["name"]: (
                f"{c['directoryPrimaryKey']['table']['dataset']['slug']}."
                f"{c['directoryPrimaryKey']['table']['slug']}:"
                f"{c['directoryPrimaryKey']['name']}"
            )
            for c in cols
            if c["directoryPrimaryKey"]
        }
        want_fks = {
            n: c["directory_column"]
            for n, c in expect.items()
            if c.get("directory_column")
        }
        ol_linked = {
            c["name"]: c["observationLevel"]["entity"]["slug"]
            for c in cols
            if c["observationLevel"]
        }
        bad_type = [
            (
                c["name"],
                c["bigqueryType"]["name"] if c["bigqueryType"] else None,
                expect[c["name"]]["bigquery_type"],
            )
            for c in cols
            if c["name"] in expect
            and (c["bigqueryType"]["name"] if c["bigqueryType"] else None)
            != expect[c["name"]]["bigquery_type"]
        ]

        total_columns += len(cols)
        total_fk += len(fks)
        total_partition += len(parts)
        total_ol_links += len(ol_linked)

        print(f"\n{slug}  table={tid}")
        print(f"  columns {len(cols)} (expected {len(expect)})")
        print(f"  cloud tables {clouds}")
        print(f"  raw sources {srcs} -> {[source_url.get(s) for s in srcs]}")
        print(f"  OLs ({len(ols)}): {sorted(ols.values())}")
        print(f"  OL-linked columns ({len(ol_linked)}): {ol_linked}")
        print(f"  partitions ({len(parts)}): {parts}")
        print(f"  primary keys ({len(pks)}): {pks}")
        print(f"  directory FKs ({len(fks)}): {fks}")
        print(f"  coverages: {covs}")
        print(f"  updates: {ups}")
        print(f"  PT-only/incomplete descriptions: {pt_only}")
        print(f"  partially-translated observations: {obs_partial}")
        if bad_type:
            print(f"  TYPE MISMATCH: {bad_type}")

        if len(cols) != len(expect):
            problems.append(
                f"{slug}: {len(cols)} columns, expected {len(expect)}"
            )
        if sorted(c["name"] for c in cols) != sorted(expect):
            problems.append(f"{slug}: column names differ from the payload")
        if pt_only:
            problems.append(
                f"{slug}: descriptions not trilingual on {pt_only}"
            )
        if obs_partial:
            problems.append(
                f"{slug}: observations not trilingual on {obs_partial}"
            )
        if pks:
            problems.append(f"{slug}: {len(pks)} primary keys set, expected 0")
        if fks != want_fks:
            problems.append(
                f"{slug}: FK mismatch stored={fks} intended={want_fks}"
            )
        if bad_type:
            problems.append(f"{slug}: bigquery type mismatch {bad_type}")
        if len(srcs) != 1:
            problems.append(
                f"{slug}: {len(srcs)} raw sources, expected exactly 1"
            )
        if len(clouds) != 1 or not clouds[0][1].startswith(
            "basedosdados.au_sa_ecsa_elections."
        ):
            problems.append(f"{slug}: cloud tables {clouds}")
        if len(covs) != 1 or len(covs[0][3]) != 1:
            problems.append(f"{slug}: coverages/ranges {covs}")
        if len(ups) != 1:
            problems.append(f"{slug}: {len(ups)} updates, expected 1")
        expect_ols = {
            e[0]
            for e in json.loads((META / "tables.json").read_text())[slug][
                "observation_levels"
            ]
        }
        if set(ols.values()) != expect_ols or len(ols) != len(expect_ols):
            problems.append(
                f"{slug}: OLs {sorted(ols.values())} expected {sorted(expect_ols)}"
            )
        expect_links = {
            c: e
            for e, c in json.loads((META / "tables.json").read_text())[slug][
                "observation_levels"
            ]
        }
        if ol_linked != expect_links:
            problems.append(
                f"{slug}: OL links {ol_linked} expected {expect_links}"
            )
        if slug != "dicionario" and parts != ["year"]:
            problems.append(f"{slug}: partitions {parts}, expected ['year']")
        if slug == "dicionario" and parts:
            problems.append(f"dicionario: partitions {parts}, expected none")

    print("\n=== totals ===")
    print(
        f"columns {total_columns}   directory FKs {total_fk}   "
        f"partitions {total_partition}   OL-linked columns {total_ol_links}"
    )
    print("\n=== problems ===")
    if problems:
        for p in problems:
            print(f"  ! {p}")
    else:
        print("  none")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
