"""Move the MG coverage of the six pre-existing MiDES tables to 2014-2026, and
attach the dataset tags the new tables made relevant.

    ~/.venvs/bd-pipelines/bin/python models/world_wb_mides/code/update_mg_coverage.py --dry-run
    ~/.venvs/bd-pipelines/bin/python models/world_wb_mides/code/update_mg_coverage.py

Coverage on MiDES is PER AREA: `empenho` carries one Coverage for `br_mg`,
another for `br_ce`, and so on, each with its own DateTimeRange. Every MG range
currently ends at 2021, which is what the tables held before this rebuild. Only
the `br_mg` range is touched here -- the other states are not ours to move.

The end year is asserted from the SOURCE, not from BigQuery: every one of the 49
staging mirrors carries exercise 2026 (verified by object count in the bucket),
so 2026 is what the rebuilt tables contain. It does depend on the rebuild having
run; until then the range is a forward claim, which is why this is a separate
script and not part of registration.

The DateTimeRange id is always reused. Omitting it creates a SECOND range, and a
table with two ranges then breaks `create_update_table` outright.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(
    0,
    os.path.expanduser("~/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"),
)

# pyrefly: ignore [missing-import]  # the databasis MCP server, via sys.path
import server

ENV = "staging"
AREA = "br_mg"
END_YEAR = 2026
DATASET_ID = "d3874769-bcbd-4ece-a38a-157ba1021514"  # slug `mides`

# NOT `get_dataset`: it returns every column of every table, which on this
# dataset takes 80+ seconds -- past the 60s read timeout inside the client
# itself. Ask for exactly the coverage ids, and the dataset's own fields.
COVERAGE_QUERY = """query($ds: ID!, $slug: String!) {
  allTable(dataset_Id: $ds, slug: $slug, first: 1) {
    edges { node { coverages { edges { node { id area { slug }
      datetimeRanges { edges { node { id startYear endYear interval } } } } } } } }
  }
}"""

DATASET_QUERY = """query($id: ID!) {
  allDataset(id: $id, first: 1) {
    edges { node { id namePt nameEn nameEs
      descriptionPt descriptionEn descriptionEs
      organizations { edges { node { id } } }
      themes { edges { node { id } } }
      tags { edges { node { id slug } } } } }
  }
}"""


def coverages_of(slug: str) -> list[dict] | None:
    edges = server._gql(
        COVERAGE_QUERY, {"ds": DATASET_ID, "slug": slug}, env=ENV
    )["allTable"]["edges"]
    if not edges:
        return None
    return [
        {
            "id": server._strip_id(e["node"]["id"]),
            "area_slug": e["node"]["area"]["slug"],
            "datetime_ranges": [
                {
                    "id": server._strip_id(r["node"]["id"]),
                    "start_year": r["node"]["startYear"],
                    "end_year": r["node"]["endYear"],
                    "interval": r["node"]["interval"],
                }
                for r in e["node"]["datetimeRanges"]["edges"]
            ],
        }
        for e in edges[0]["node"]["coverages"]["edges"]
    ]


def dataset_fields() -> dict:
    node = server._gql(DATASET_QUERY, {"id": DATASET_ID}, env=ENV)[
        "allDataset"
    ]["edges"][0]["node"]
    return {
        "id": server._strip_id(node["id"]),
        "name_pt": node["namePt"],
        "name_en": node["nameEn"],
        "name_es": node["nameEs"],
        "description_pt": node["descriptionPt"],
        "description_en": node["descriptionEn"],
        "description_es": node["descriptionEs"],
        "organizations": [
            {"id": server._strip_id(e["node"]["id"])}
            for e in node["organizations"]["edges"]
        ],
        "themes": [
            {"id": server._strip_id(e["node"]["id"])}
            for e in node["themes"]["edges"]
        ],
        "tags": [
            {
                "id": server._strip_id(e["node"]["id"]),
                "slug": e["node"]["slug"],
            }
            for e in node["tags"]["edges"]
        ],
    }


# The six tables that carried MG before this work. The 43 new ones are
# registered with 2014-2026 from the start.
EXISTING = [
    "empenho",
    "liquidacao",
    "pagamento",
    "licitacao",
    "licitacao_item",
    "licitacao_participante",
]

# Content tags the new tables make relevant. The dataset already carries
# compra / despesa / gasto / orcamento, which say nothing about contracts or
# procurement detail. Geography, theme and organization are deliberately NOT
# tagged -- they are separate metadata fields.
ADD_TAGS = {
    "contrato": "0831b835-2079-44f3-b5e8-3f598435bbe0",
    "licitacao": "4b76d0d7-7a4b-4a73-a2c5-33a08853dc77",
    "financas_publicas": "5dce4b1d-131b-452a-a419-bdd587a8c272",
    "transparencia": "8b187427-519e-48cb-b0a6-5380086edf3b",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("MG coverage:")
    for slug in EXISTING:
        covs = coverages_of(slug)
        if covs is None:
            print(f"  {slug:<26} not registered, skipped")
            continue
        mg = next((c for c in covs if c.get("area_slug") == AREA), None)
        if not mg:
            print(f"  {slug:<26} has no {AREA} coverage, skipped")
            continue
        ranges = mg.get("datetime_ranges") or []
        if not ranges:
            print(f"  {slug:<26} {AREA} coverage has no range, skipped")
            continue
        current = ranges[0]
        start = current.get("start_year")
        if current.get("end_year") == END_YEAR:
            print(f"  {slug:<26} already {start}-{END_YEAR}")
            continue
        if args.dry_run:
            print(
                f"  {slug:<26} {start}-{current.get('end_year')} -> {start}-{END_YEAR}"
            )
            continue
        server.create_update_datetime_range(
            coverage_id=mg["id"],
            start_year=start,
            end_year=END_YEAR,
            interval=current.get("interval", 1),
            id=current["id"],  # never omit: a second range breaks the table
            env=ENV,
        )
        print(
            f"  {slug:<26} {start}-{current.get('end_year')} -> {start}-{END_YEAR}  updated"
        )

    dataset = dataset_fields()
    have = {t["slug"] for t in dataset.get("tags", [])}
    missing = {k: v for k, v in ADD_TAGS.items() if k not in have}
    print(f"\ntags: dataset has {sorted(have)}")
    if not missing:
        print("  nothing to add")
        return
    print(f"  adding {sorted(missing)}")
    if args.dry_run:
        return
    server.create_update_dataset(
        id=dataset["id"],
        slug="mides",
        name_pt=dataset["name_pt"],
        name_en=dataset["name_en"],
        name_es=dataset["name_es"],
        description_pt=dataset["description_pt"],
        description_en=dataset["description_en"],
        description_es=dataset["description_es"],
        organization_ids=[o["id"] for o in dataset["organizations"]],
        theme_ids=[t["id"] for t in dataset["themes"]],
        # M2M: the full desired set, not a delta -- the API does no partial update
        tag_ids=[t["id"] for t in dataset.get("tags", [])]
        + list(missing.values()),
        status_id="e16221de-ac30-4926-83d3-de219998dab3",
        env=ENV,
    )
    after = dataset_fields()
    print(f"  now: {sorted(t['slug'] for t in after['tags'])}")


if __name__ == "__main__":
    main()
