"""Register br_ufmg_censo_demografico_1872 table metadata in the Data Basis backend.

Driven by ``columns.json``, so names, descriptions, types and units cannot drift
from the architecture and the dbt models.

This talks to the backend's GraphQL API directly, reusing the databasis MCP
server's credential handling, rather than going through the MCP tools. Two of the
fields this dataset needs cannot be set any other way:

- ``bigqueryType`` — ``bulk_upsert_columns`` silently defaults every column to
  STRING, which would type all 1,100-odd count columns wrongly, and
  ``update_column`` does not expose the field at all. Only
  ``upload_columns_from_sheet`` sets it, and that requires a public Google
  Sheet per table.
- ``observationLevel`` and ``isPartition`` — likewise absent from the bulk path.

It is idempotent: every record is looked up first and updated in place, so a
re-run after a partial failure does not duplicate anything.

Run:
    uv run models/br_ufmg_censo_demografico_1872/code/register_metadata.py \
        --env staging [--dry-run] [table_slug ...]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from string import Template

sys.path.insert(0, str(Path(__file__).resolve().parent))

import databasis_mcp.tools.metadata as bd_mcp_metadata
import databasis_mcp.tools.write as bd_mcp_write

from models.br_ufmg_censo_demografico_1872.code.spec import (
    ANO,
    LEVELS,
    table_slug,
)
from models.br_ufmg_censo_demografico_1872.code.spec import (
    TABLES as SOURCE_TABLES,
)
from models.br_ufmg_censo_demografico_1872.code.tables import (
    AUXILIARES,
    DATASET_ID,
    DATASET_SLUG,
)

HERE = Path(__file__).resolve().parent

# The dataset and its raw data source already exist; this script never creates
# or modifies either.
DATASET_UUID = "1eeb071e-cfe0-4a32-a664-8e86fcb49971"
RAW_DATA_SOURCE_UUID = "bdbb608d-b5e5-4da7-b48a-fd9a5913a204"
AREA_BR = "5503dd29-4d9b-483b-ae09-63dc8ed28875"

# Per-environment. The dataset, raw source and area happen to share ids across
# backends; the account does not, and the GCP project must not. Entity ids are
# resolved by slug at runtime, which matters because `province` differs between
# staging and prod.
ENVS = {
    "staging": {"account": "57", "gcp_project": "basedosdados-dev"},
    "prod": {"account": "4", "gcp_project": "basedosdados"},
    "dev": {"account": "57", "gcp_project": "basedosdados-dev"},
}

# The bundle lives in the dev bucket: the data-uploader service account is 403
# on gs://basedosdados, so prod points at the same object rather than at a
# prod-bucket URL with nothing behind it. Both buckets are requester-pays, so
# this URL is HTTP 400 for anonymous readers either way -- a known bug that
# already affects every production table using this field.
AUX_URL = (
    "https://storage.googleapis.com/basedosdados-dev/auxiliary_files/"
    f"{DATASET_ID}/auxiliary_files.zip"
)
# Wall-clock date this dataset was last refreshed at Data Basis. The census
# itself is frozen, so this is an onboarding date, not a date in the data.
REFRESHED = f"{2026}-09-28T00:00:00"

# Which entity each geography column identifies.
COLUMN_ENTITY = {
    "ano": "year",
    "id_provincia": "province",
    "id_municipio_1872": "municipality",
    "id_paroquia": "parish",
}
# Observation levels per published table, in order.
LEVEL_ENTITIES = {
    "paroquia": ["year", "province", "municipality", "parish"],
    "municipio": ["year", "province", "municipality"],
    "provincia": ["year", "province"],
}
AUX_ENTITIES = {
    "provincia": ["year", "province"],
    "municipio": ["year", "province", "municipality"],
    "paroquia": ["year", "province", "municipality", "parish"],
    "dicionario": [],
}


Q_ALL = Template("{ $root { edges { node { id $key } } } }")
Q_TABLE = Template(
    '{ allTable(dataset_Id: "$ds", slug: "$slug") { edges { node { id } } } }'
)
Q_COLUMNS = Template(
    '{ allColumn(table_Id: "$table") { edges { node { id name } } } }'
)
Q_OLS = Template(
    '{ allObservationlevel(table_Id: "$table") '
    "{ edges { node { id entity { slug } } } } }"
)
Q_BY_TABLE = Template(
    '{ $root(table_Id: "$table") { edges { node { id } } } }'
)
Q_DATETIME = Template(
    '{ allDatetimerange(coverage_Id: "$coverage") { edges { node { id } } } }'
)


class Backend:
    def __init__(self, env: str, dry_run: bool) -> None:
        self.env = env
        self.dry_run = dry_run
        self._fk_cache: dict[str, str | None] = {}
        if env not in ENVS:
            raise SystemExit(
                f"unknown env {env!r}; expected one of {sorted(ENVS)}"
            )
        self.account = ENVS[env]["account"]
        self.gcp_project = ENVS[env]["gcp_project"]
        self.entities = self._map("allEntity", "slug")
        # BigQueryType keys on `name` ("INT64"), not `slug` -- it has none.
        self.bq_types = self._map("allBigquerytype", "name")
        self.published = self._map("allStatus", "slug")["published"]

    def query(self, body: Template, **args: str) -> dict:
        """Run a read query. Templates keep GraphQL's braces readable, which
        neither %-formatting nor f-strings manage here."""
        return bd_mcp_metadata._gql(
            body.substitute(**args), env=self.env, auth=False
        )

    def _map(self, root: str, key: str) -> dict[str, str]:
        d = self.query(Q_ALL, root=root, key=key)
        return {
            e["node"][key]: bd_mcp_metadata._strip_id(e["node"]["id"])
            for e in d[root]["edges"]
        }

    def mut(self, name: str, fields: dict, result: str = "") -> dict:
        if self.dry_run:
            print(
                f"    [dry-run] {name} {json.dumps(fields, ensure_ascii=False)[:150]}"
            )
            return {}
        return bd_mcp_write._mut(name, fields, result or "", env=self.env)

    # -- lookups -----------------------------------------------------------
    def table_id(self, slug: str) -> str | None:
        d = self.query(Q_TABLE, ds=DATASET_UUID, slug=slug)
        edges = d["allTable"]["edges"]
        return (
            bd_mcp_metadata._strip_id(edges[0]["node"]["id"])
            if edges
            else None
        )

    def columns(self, table: str) -> dict[str, str]:
        d = self.query(Q_COLUMNS, table=table)
        return {
            e["node"]["name"]: bd_mcp_metadata._strip_id(e["node"]["id"])
            for e in d["allColumn"]["edges"]
        }

    def observation_levels(self, table: str) -> dict[str, str]:
        d = self.query(Q_OLS, table=table)
        return {
            e["node"]["entity"]["slug"]: bd_mcp_metadata._strip_id(
                e["node"]["id"]
            )
            for e in d["allObservationlevel"]["edges"]
        }

    def one(self, root: str, table: str) -> str | None:
        d = self.query(Q_BY_TABLE, root=root, table=table)
        edges = d[root]["edges"]
        return (
            bd_mcp_metadata._strip_id(edges[0]["node"]["id"])
            if edges
            else None
        )

    def directory_fk(self, spec: str) -> str | None:
        """Resolve "<dataset>.<table>:<column>" to that column's backend id.

        Cached, and tolerant of a miss: if the directory is absent from this
        backend the column simply carries no FK rather than failing the run.
        """
        if not spec:
            return None
        if spec not in self._fk_cache:
            try:
                self._fk_cache[spec] = bd_mcp_write._lookup_directory_column(
                    spec, self.env
                )
            except Exception:
                self._fk_cache[spec] = None
        return self._fk_cache[spec]

    def datetime_range(self, coverage: str) -> str | None:
        d = self.query(Q_DATETIME, coverage=coverage)
        edges = d["allDatetimerange"]["edges"]
        return (
            bd_mcp_metadata._strip_id(edges[0]["node"]["id"])
            if edges
            else None
        )


def register_table(
    be: Backend, slug: str, meta: dict, entities: list[str]
) -> None:
    print(f"  {slug}")

    # 1. the table itself
    fields = {
        "slug": slug,
        "dataset": DATASET_UUID,
        # The API requires a single `name` alongside the per-language ones.
        "name": meta["name_pt"],
        "namePt": meta["name_pt"],
        "nameEn": meta["name_en"],
        "nameEs": meta["name_es"],
        "descriptionPt": meta["description_pt"],
        "descriptionEn": meta["description_en"],
        "descriptionEs": meta["description_es"],
        "status": be.published,
        "publishedBy": [be.account],
        "dataCleanedBy": [be.account],
        "rawDataSource": [RAW_DATA_SOURCE_UUID],
        "auxiliaryFilesUrl": AUX_URL,
        "numberRows": meta.get("number_rows"),
        "numberColumns": len(meta["columns"]),
    }
    existing = be.table_id(slug)
    if existing:
        fields["id"] = existing
    out = be.mut("CreateUpdateTable", fields, "table { id }")
    table = existing or (
        bd_mcp_metadata._strip_id(out["table"]["id"]) if out else "DRY"
    )
    if be.dry_run:
        return

    # 2. observation levels
    have_ol = be.observation_levels(table)
    ol_ids: dict[str, str] = {}
    for ent in entities:
        f = {"table": table, "entity": be.entities[ent]}
        if ent in have_ol:
            ol_ids[ent] = have_ol[ent]
            continue
        o = be.mut(
            "CreateUpdateObservationLevel", f, "observationlevel { id }"
        )
        ol_ids[ent] = bd_mcp_metadata._strip_id(o["observationlevel"]["id"])

    # 3. columns — one mutation each, because bigqueryType, isPartition and
    #    observationLevel are only settable here.
    have_cols = be.columns(table)
    for col in meta["columns"]:
        f = {
            "table": table,
            "name": col["name"],
            "bigqueryType": be.bq_types[col["bigquery_type"]],
            "descriptionPt": col["description_pt"],
            "descriptionEn": col["description_en"],
            "descriptionEs": col["description_es"],
            "coveredByDictionary": col["covered_by_dictionary"],
            "measurementUnit": col["measurement_unit"],
            "isPartition": col["is_partition"],
            "isPrimaryKey": False,
        }
        for lang in ("pt", "en", "es"):
            note = col.get(f"observations_{lang}")
            if note:
                f[f"observations{lang.capitalize()}"] = note
        ent = COLUMN_ENTITY.get(col["name"])
        if ent and ent in ol_ids:
            f["observationLevel"] = ol_ids[ent]
        fk = be.directory_fk(col.get("directory_column", ""))
        if fk:
            f["directoryPrimaryKey"] = fk
        if col["name"] in have_cols:
            f["id"] = have_cols[col["name"]]
        be.mut("CreateUpdateColumn", f, "column { id }")

    # 4. cloud table
    ct = be.one("allCloudtable", table)
    f = {
        "table": table,
        "gcpProjectId": be.gcp_project,
        "gcpDatasetId": DATASET_ID,
        "gcpTableId": slug,
    }
    if ct:
        f["id"] = ct
    be.mut("CreateUpdateCloudTable", f, "cloudtable { id }")

    # 5. coverage + datetime range (annual, single year)
    cov = be.one("allCoverage", table)
    f = {"table": table, "area": AREA_BR}
    if cov:
        f["id"] = cov
    else:
        # pyrefly: ignore [bad-assignment]
        f["isClosed"] = False
    o = be.mut("CreateUpdateCoverage", f, "coverage { id }")
    cov = cov or bd_mcp_metadata._strip_id(o["coverage"]["id"])

    dr = be.datetime_range(cov)
    f = {
        "coverage": cov,
        "startYear": ANO,
        "endYear": ANO,
        "interval": 1,
        "isClosed": True,
    }
    if dr:
        f["id"] = dr
    be.mut("CreateUpdateDateTimeRange", f, "datetimerange { id }")

    # 6. update record — when Data Basis last refreshed this table
    up = be.one("allUpdate", table)
    f = {
        "table": table,
        "entity": be.entities["year"],
        "frequency": 1,
        "latest": REFRESHED,
    }
    if up:
        f["id"] = up
    be.mut("CreateUpdateUpdate", f, "update { id }")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("slugs", nargs="*")
    args = ap.parse_args()

    meta_all = json.loads((HERE / "columns.json").read_text())
    rows = json.loads((HERE / "row_counts.json").read_text())

    order: list[tuple[str, list[str]]] = [
        (a, AUX_ENTITIES[a]) for a in AUXILIARES
    ]
    for source in SOURCE_TABLES:
        for level in LEVELS:
            order.append((table_slug(source, level), LEVEL_ENTITIES[level]))

    todo = [(s, e) for s, e in order if not args.slugs or s in args.slugs]
    print(f"=== registering {len(todo)} tables in {args.env} ===", flush=True)

    be = Backend(args.env, args.dry_run)
    for i, (slug, entities) in enumerate(todo, 1):
        meta = dict(meta_all[slug])
        meta["number_rows"] = rows.get(slug)
        print(f"[{i}/{len(todo)}]", end=" ", flush=True)
        register_table(be, slug, meta, entities)

    print(f"\nDone. Verify at the {args.env} frontend: {DATASET_SLUG}")


if __name__ == "__main__":
    main()
