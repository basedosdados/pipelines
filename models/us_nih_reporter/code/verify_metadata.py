"""Read the registered us_nih_reporter metadata back and check it.

The MCP write tools return only an id, so nothing above confirms that a type, a
measurement unit, a directory FK or an observation-level link actually landed.
This reads them through GraphQL, which is the only way to see them, and checks
for the duplicate records create_update_* produces when called without an id.
"""

import sys
from collections import Counter

from common import import_mcp_server

server = import_mcp_server()

ENV = sys.argv[1] if len(sys.argv) > 1 else "staging"
SLUG = "nih_reporter"

ds = server.get_dataset(slug=SLUG, env=ENV)
print(f"dataset {SLUG} id={ds['id']}  found={ds['found']}")
print(f"  orgs={[o['slug'] for o in ds['organizations']]}")
print(f"  themes={[t['slug'] for t in ds['themes']]}")
print(f"  tags={sorted(t['slug'] for t in ds['tags'])}")

q = """
query($id: ID!) {
  allDataset(id: $id) { edges { node {
    slug status { slug }
    tables { edges { node {
      id slug order auxiliaryFilesUrl
      status { slug }
      rawDataSource { edges { node { id url } } }
      observationLevels { edges { node { id entity { slug }
        columns { edges { node { name } } } } } }
      cloudTables { edges { node { id gcpProjectId gcpDatasetId gcpTableId } } }
      coverages { edges { node { id isClosed area { slug }
        datetimeRanges { edges { node { id startYear endYear interval isClosed } } } } } }
      updates { edges { node { id entity { slug } frequency latest } } }
    } } }
  } } }
}
"""
node = server._gql(q, {"id": ds["id"]}, env=ENV)["allDataset"]["edges"][0][
    "node"
]
print(f"  status={node['status']['slug']}")

problems = []
for e in sorted(
    node["tables"]["edges"], key=lambda e: e["node"]["order"] or 0
):
    t = e["node"]
    tid = server._strip_id(t["id"])
    ols = [o["node"] for o in t["observationLevels"]["edges"]]
    cts = [c["node"] for c in t["cloudTables"]["edges"]]
    covs = [c["node"] for c in t["coverages"]["edges"]]
    ups = [u["node"] for u in t["updates"]["edges"]]
    raws = [r["node"]["url"] for r in t["rawDataSource"]["edges"]]
    print(
        f"\n-- {t['slug']} (order={t['order']}, status={t['status']['slug']})"
    )
    print(f"   raw sources: {raws}")
    print(f"   aux: {t['auxiliaryFilesUrl']}")
    for o in ols:
        cols = [c["node"]["name"] for c in o["columns"]["edges"]]
        print(f"   OL {o['entity']['slug']:<10} columns={cols}")
        if not cols:
            problems.append(
                f"{t['slug']}: OL {o['entity']['slug']} has no column"
            )
    for c in cts:
        print(
            f"   cloud {c['gcpProjectId']}.{c['gcpDatasetId']}.{c['gcpTableId']}"
        )
    for c in covs:
        rng = [r["node"] for r in c["datetimeRanges"]["edges"]]
        print(
            f"   coverage area={c['area']['slug']} is_closed={c['isClosed']} ranges={[(r['startYear'], r['endYear']) for r in rng]}"
        )
        if len(rng) > 1:
            problems.append(f"{t['slug']}: {len(rng)} datetime ranges")
    for u in ups:
        print(
            f"   update entity={u['entity']['slug']} freq={u['frequency']} latest={u['latest']}"
        )
    for label, items in (
        ("cloud table", cts),
        ("coverage", covs),
        ("update", ups),
    ):
        if len(items) > 1:
            problems.append(
                f"{t['slug']}: {len(items)} {label} records (duplicate)"
            )
    if len(raws) > 1:
        problems.append(
            f"{t['slug']}: {len(raws)} raw sources — breaks the pipeline poll"
        )

    # columns
    cq = """
    query($id: ID!) {
      allColumn(table_Id: $id, first: 200) { edges { node {
        name bigqueryType { name } measurementUnit isPartition isPrimaryKey
        coveredByDictionary containsSensitiveData
        directoryPrimaryKey { name table { slug dataset { slug } } }
        observationLevel { entity { slug } }
        descriptionPt descriptionEn descriptionEs
        observationsPt observationsEn observationsEs
      } } }
    }
    """
    cols = [
        c["node"]
        for c in server._gql(cq, {"id": tid}, env=ENV)["allColumn"]["edges"]
    ]
    types = Counter(
        c["bigqueryType"]["name"] if c["bigqueryType"] else None for c in cols
    )
    part = [c["name"] for c in cols if c["isPartition"]]
    pk = [c["name"] for c in cols if c["isPrimaryKey"]]
    units = {
        c["name"]: c["measurementUnit"] for c in cols if c["measurementUnit"]
    }
    dicts = [c["name"] for c in cols if c["coveredByDictionary"]]
    fks = {
        c["name"]: f"{c['directoryPrimaryKey']['table']['dataset']['slug']}."
        f"{c['directoryPrimaryKey']['table']['slug']}:{c['directoryPrimaryKey']['name']}"
        for c in cols
        if c["directoryPrimaryKey"]
    }
    missing_en = [c["name"] for c in cols if not c["descriptionEn"]]
    missing_es = [c["name"] for c in cols if not c["descriptionEs"]]
    obs_pt = [c["name"] for c in cols if c["observationsPt"]]
    obs_missing = [
        c["name"]
        for c in cols
        if c["observationsPt"]
        and not (c["observationsEn"] and c["observationsEs"])
    ]
    print(f"   {len(cols)} columns, types={dict(types)}")
    print(f"   partition={part} primary_key={pk}")
    print(f"   units={units}")
    print(f"   dictionary={dicts}")
    print(f"   directory FKs={fks}")
    print(
        f"   observations: {len(obs_pt)} columns; missing EN/ES on {obs_missing}"
    )
    if missing_en or missing_es:
        problems.append(
            f"{t['slug']}: missing EN/ES description on {missing_en or missing_es}"
        )
    if obs_missing:
        problems.append(f"{t['slug']}: PT-only observations on {obs_missing}")
    if None in types:
        problems.append(
            f"{t['slug']}: {types[None]} columns with no bigquery_type"
        )
    if pk:
        problems.append(
            f"{t['slug']}: is_primary_key set outside a directory table: {pk}"
        )

print("\n=== raw sources ===")
rq = """
query($id: ID!) {
  allRawdatasource(id: $id) { edges { node {
    id url license { slug } availability { slug } containsApi
    updates { edges { node { entity { slug } frequency latest } } }
    polls { edges { node { entity { slug } latest } } }
  } } }
}
"""
listed = server.get_raw_data_sources(dataset_slug=SLUG, env=ENV)
if isinstance(listed, dict):
    listed = listed.get("result", [])
for s in listed:
    n = server._gql(rq, {"id": s["id"]}, env=ENV)["allRawdatasource"]["edges"][
        0
    ]["node"]
    ups = [u["node"] for u in n["updates"]["edges"]]
    print(
        f"  {s['url']}\n     license={n['license']['slug'] if n['license'] else None} "
        f"availability={n['availability']['slug'] if n['availability'] else None} "
        f"api={n['containsApi']} updates={[(u['entity']['slug'], u['latest']) for u in ups]}"
    )

print("\n=== verdict ===")
if problems:
    for p in problems:
        print(f"  FAIL {p}")
    sys.exit(1)
print("  PASS")
