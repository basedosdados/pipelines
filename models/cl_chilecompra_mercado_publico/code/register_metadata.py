"""Register cl_chilecompra_mercado_publico metadata in the Data Basis backend.

    python models/cl_chilecompra_mercado_publico/code/register_metadata.py --env staging
    python models/cl_chilecompra_mercado_publico/code/register_metadata.py --env prod

The whole registration is one idempotent script rather than a hand-driven sequence
of MCP calls, for two reasons:

1. ``create_update_observation_level`` / ``_cloud_table`` / ``_coverage`` / ``_update``
   are NOT idempotent -- called without an ``id`` they create a duplicate. Every step
   here reads the current state first and reuses the id it finds.
2. Promotion to prod is then a re-run against the other backend with the same spec,
   instead of 200-odd calls repeated by hand.

It talks to the backend through the databasis MCP server's own functions, imported
directly, so a 43 KB column payload never has to be pasted through a tool call.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

CODE = Path(__file__).resolve().parent
sys.path.insert(0, str(CODE))
sys.path.insert(
    0, str(Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp")
)

import metadata_spec as spec  # noqa: E402
import server  # noqa: E402

GCP_PROJECT = {
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
    "prod": "basedosdados",
}


def shift_month(ym: tuple[int, int], months: int) -> tuple[int, int]:
    total = ym[0] * 12 + (ym[1] - 1) + months
    return total // 12, total % 12 + 1


class Registrar:
    def __init__(self, env: str, dry_run: bool = False, publish: bool = False):
        self.env = env
        self.dry_run = dry_run
        # A dataset is registered under_review so it cannot leak to the public site
        # before its PR has merged and the prod tables exist. dev/staging is not the
        # public site, so it is published before the review checkpoint; prod is
        # published only after merge + table-approve + verification.
        self.publish = publish
        self.ids: dict[str, dict[str, str]] = {}
        self.account = server.get_authenticated_account(env=env)["id"]

    # ---------------------------------------------------------------- helpers
    def log(self, *parts):
        print(" ".join(str(p) for p in parts), flush=True)

    @property
    def tags(self) -> list[str]:
        return spec.tags_for(self.env)

    def ensure_organization_and_license(self):
        """Create the ChileCompra organization and the libre_uso_cl licence if absent.

        Both are this dataset's own records -- ChileCompra publishes no open licence, so
        its terms get a record of their own mirroring libre_uso_mx -- and neither exists
        on a backend where this dataset has not been registered before. Creating them is
        idempotent: create_update_* keyed by slug updates the row if it is already there.

        A dry run records a placeholder id for whatever it would have created, so
        resolve_ids does not then abort on "missing organization/license" and hide the
        rest of the plan -- which is the whole point of asking for a dry run.
        """
        if spec.LICENSE not in self.ids["license"]:
            if self.dry_run:
                self.ids["license"][spec.LICENSE] = "<dry-run>"
                self.log(f"  would create licence {spec.LICENSE}")
            else:
                made = server.create_update_license(
                    env=self.env, **spec.LICENSE_RECORD
                )
                self.ids["license"][spec.LICENSE] = made["id"]
                self.log(f"  created licence {spec.LICENSE}: {made['id']}")
        if spec.ORGANIZATION not in self.ids["organization"]:
            if self.dry_run:
                self.ids["organization"][spec.ORGANIZATION] = "<dry-run>"
                self.log(f"  would create organization {spec.ORGANIZATION}")
            else:
                area = server.lookup_id(
                    category="area", slug=spec.AREA, env=self.env
                )
                made = server.create_update_organization(
                    env=self.env,
                    area_id=area["id"],
                    **spec.ORGANIZATION_RECORD,
                )
                self.ids["organization"][spec.ORGANIZATION] = made["id"]
                self.log(
                    f"  created organization {spec.ORGANIZATION}: {made['id']}"
                )

    def resolve_ids(self):
        keys = [
            "status",
            "entity",
            "organization",
            "theme",
            "tag",
            "license",
            "availability",
        ]
        found = server.discover_ids(env=self.env, keys=keys)
        for key in keys:
            self.ids[key] = found.get(key, {})
        # discover_ids deliberately excludes "area" -- it is looked up one slug
        # at a time.
        area = server.lookup_id(category="area", slug=spec.AREA, env=self.env)
        self.ids["area"] = {spec.AREA: area["id"]} if area.get("id") else {}
        # The organization and the licence are this dataset's own records, and are
        # created on a backend that lacks them. Everything else must already exist:
        # inventing a theme or a tag silently forks a shared vocabulary.
        self.ensure_organization_and_license()

        missing = []
        for key, slug in (
            [
                ("organization", spec.ORGANIZATION),
                ("license", spec.LICENSE),
                ("availability", spec.AVAILABILITY),
                ("area", spec.AREA),
            ]
            + [("theme", t) for t in spec.THEMES]
            + [("tag", t) for t in self.tags]
        ):
            if slug not in self.ids[key]:
                missing.append(f"{key}:{slug}")
        entities = {e for t in spec.TABLES for e in t["observation_levels"]}
        missing += [
            f"entity:{e}" for e in entities if e not in self.ids["entity"]
        ]
        if missing:
            raise SystemExit(
                f"missing reference ids on env={self.env}: {', '.join(missing)}"
            )

    def state(self) -> dict:
        return server.get_dataset(spec.DATASET_SLUG, env=self.env)

    def coverage_state(self, table_id: str) -> list[dict]:
        """Coverages with isClosed, which get_dataset does not return."""
        q = """
        query($id: ID!) {
          allTable(id: $id) { edges { node { coverages(first: 10) { edges { node {
            id isClosed area { slug }
            datetimeRanges(first: 10) { edges { node {
              id startYear startMonth endYear endMonth
            } } }
          } } } } } }
        }"""
        data = server._gql(q, {"id": table_id}, env=self.env)
        edges = data["allTable"]["edges"]
        if not edges:
            return []
        out = []
        for cov in edges[0]["node"]["coverages"]["edges"]:
            node = cov["node"]
            out.append(
                {
                    "id": server._strip_id(node["id"]),
                    "is_closed": bool(node["isClosed"]),
                    "area_slug": (node.get("area") or {}).get("slug"),
                    "ranges": [
                        {
                            "id": server._strip_id(r["node"]["id"]),
                            "start": (
                                r["node"]["startYear"],
                                r["node"]["startMonth"],
                            ),
                            "end": (
                                r["node"]["endYear"],
                                r["node"]["endMonth"],
                            ),
                        }
                        for r in node["datetimeRanges"]["edges"]
                    ],
                }
            )
        return out

    # ------------------------------------------------------------- 1. dataset
    def dataset(self) -> str:
        current = self.state()
        args = dict(
            slug=spec.DATASET_SLUG,
            **{k: v for k, v in spec.DATASET.items()},
            organization_ids=[self.ids["organization"][spec.ORGANIZATION]],
            theme_ids=[self.ids["theme"][t] for t in spec.THEMES],
            tag_ids=[self.ids["tag"][t] for t in self.tags],
            status_id=self.ids["status"][
                "published" if self.publish else "under_review"
            ],
            env=self.env,
        )
        if current["found"]:
            args["id"] = current["id"]
        status = "published" if self.publish else "under_review"
        if self.dry_run:
            self.log(
                "dataset:",
                "update" if current["found"] else "create",
                f"({status})",
            )
            return current["id"] or ""
        result = server.create_update_dataset(**args)
        self.log("dataset:", result["id"], f"({status})")
        return result["id"]

    # --------------------------------------------------------- 2. raw sources
    def raw_sources(self, dataset_id: str) -> dict[str, str]:
        existing = {
            s["name"]: s["id"]
            for s in server.get_raw_data_sources(
                spec.DATASET_SLUG, env=self.env
            )
        }
        out = {}
        for key, src in spec.RAW_SOURCES.items():
            args = dict(
                dataset_id=dataset_id,
                license_id=self.ids["license"][spec.LICENSE],
                availability_id=self.ids["availability"][spec.AVAILABILITY],
                has_structured_data=True,
                is_free=True,
                contains_api=False,
                requires_registration=False,
                status_id=self.ids["status"]["under_review"],
                env=self.env,
                **src,
            )
            existing_id = existing.get(src["name_pt"], "")
            if existing_id:
                args["id"] = existing_id
            out[key] = (
                existing_id
                if self.dry_run
                else server.create_update_raw_data_source(**args)["id"]
            )
            self.log(f"  raw source {key}: {out[key]}")
        return out

    # -------------------------------------------------------------- 3. tables
    def tables(
        self, dataset_id: str, raw_ids: dict[str, str]
    ) -> dict[str, str]:
        current = self.state()["tables"]
        out = {}
        for table in spec.TABLES:
            slug = table["slug"]
            args = dict(
                slug=slug,
                name_pt=table["name_pt"],
                name_en=table["name_en"],
                name_es=table["name_es"],
                description_pt=table["description_pt"],
                description_en=table["description_en"],
                description_es=table["description_es"],
                dataset_id=dataset_id,
                status_id=self.ids["status"]["published"],
                published_by_ids=[self.account],
                data_cleaned_by_ids=[self.account],
                env=self.env,
            )
            # One raw source per table, never two: client._raw_source_id raises on a
            # table with more than one, which would make the recurring pipeline's poll
            # fail before it did anything.
            if table["raw_source"]:
                args["raw_data_source_ids"] = [raw_ids[table["raw_source"]]]
            if table["auxiliary_files"]:
                args["auxiliary_files_url"] = spec.auxiliary_files_url(slug)
            existing_id = current[slug]["id"] if slug in current else ""
            if existing_id:
                args["id"] = existing_id
            out[slug] = (
                existing_id
                if self.dry_run
                else server.create_update_table(**args)["id"]
            )
            self.log(f"  table {slug}: {out[slug]}")
        return out

    # ------------------------------------------------------------- 4. columns
    def columns(self, table_ids: dict[str, str]):
        for table in spec.TABLES:
            slug = table["slug"]
            if slug == "dicionario":
                payload = spec.DICIONARIO_COLUMNS
            else:
                payload = json.loads(
                    (CODE / f"columns_{slug}.json").read_text(encoding="utf-8")
                )
            if self.dry_run:
                self.log(f"  {slug}: {len(payload)} columns (dry run)")
                continue
            result = server.bulk_upsert_columns(
                table_id=table_ids[slug],
                columns_json=json.dumps(payload, ensure_ascii=False),
                env=self.env,
            )
            errors = result.get("errors") or []
            self.log(
                f"  {slug}: created={result.get('created', 0)} "
                f"updated={result.get('updated', 0)} errors={len(errors)}"
            )
            if errors:
                raise SystemExit(f"{slug}: {errors[:3]}")

    # -------------------------------------------- 5. observation levels + links
    def observation_levels(self, table_ids: dict[str, str]):
        current = self.state()["tables"]
        for table in spec.TABLES:
            slug = table["slug"]
            if not table["observation_levels"]:
                continue
            have = {
                ol["entity_slug"]: ol["id"]
                for ol in current.get(slug, {}).get("observation_levels", [])
            }
            ol_ids = {}
            for entity in table["observation_levels"]:
                if entity in have:
                    ol_ids[entity] = have[entity]
                elif self.dry_run:
                    ol_ids[entity] = ""
                else:
                    ol_ids[entity] = server.create_update_observation_level(
                        table_id=table_ids[slug],
                        entity_id=self.ids["entity"][entity],
                        env=self.env,
                    )["id"]
            self.log(f"  {slug}: {len(ol_ids)} observation levels")
            if not self.dry_run:
                server.reorder_observation_levels(
                    table_id=table_ids[slug],
                    ol_ids=[ol_ids[e] for e in table["observation_levels"]],
                    env=self.env,
                )

            # Link each identifying column to its level, or the site renders the
            # level's columns as "Nao informado". update_column's booleans default to
            # False, so is_partition has to be re-passed in the same call.
            by_name = {
                c["name"]: c["id"]
                for c in current.get(slug, {}).get("columns", [])
            }
            linked = 0
            for column, entity in table["observation_level_columns"].items():
                if column not in by_name:
                    raise SystemExit(f"{slug}: column {column} not registered")
                if self.dry_run:
                    linked += 1
                    continue
                server.update_column(
                    column_id=by_name[column],
                    column_name=column,
                    table_id=table_ids[slug],
                    observation_level_id=ol_ids[entity],
                    is_partition=column in spec.PARTITION_COLUMNS,
                    env=self.env,
                )
                linked += 1
            self.log(f"  {slug}: {linked} columns linked to a level")

    # --------------------------------------------------------- 6. cloud tables
    def cloud_tables(self, table_ids: dict[str, str]):
        current = self.state()["tables"]
        project = GCP_PROJECT[self.env]
        for table in spec.TABLES:
            slug = table["slug"]
            have = current.get(slug, {}).get("cloud_tables", [])
            args = dict(
                table_id=table_ids[slug],
                gcp_project_id=project,
                gcp_dataset_id=spec.GCP_DATASET_ID,
                gcp_table_id=slug,
                env=self.env,
            )
            if have:
                args["id"] = have[0]["id"]
            if self.dry_run:
                self.log(
                    f"  {slug}: cloud table -> {project}.{spec.GCP_DATASET_ID}"
                )
                continue
            result = server.create_update_cloud_table(**args)
            self.log(f"  {slug}: cloud table {result['id']}")

    # ------------------------------------------------------------ 7. coverage
    def coverage(self, table_ids: dict[str, str]):
        area_id = self.ids["area"][spec.AREA]
        free_end = shift_month(spec.COVERAGE_END, -spec.FREE_LAG_MONTHS)
        pro_start = shift_month(free_end, 1)
        for table in spec.TABLES:
            slug = table["slug"]
            monthly = slug != "dicionario"
            have = self.coverage_state(table_ids[slug])
            by_closed = {c["is_closed"]: c for c in have}

            wanted = [
                (False, spec.COVERAGE_START, free_end if monthly else None)
            ]
            if monthly:
                wanted.append((True, pro_start, spec.COVERAGE_END))

            for is_closed, start, end in wanted:
                cov = by_closed.get(is_closed)
                cov_args = dict(
                    table_id=table_ids[slug],
                    area_id=area_id,
                    is_closed=is_closed,
                    env=self.env,
                )
                if cov:
                    cov_args["id"] = cov["id"]
                if self.dry_run:
                    self.log(
                        f"  {slug}: coverage closed={is_closed} {start}..{end}"
                    )
                    continue
                cov_id = server.create_update_coverage(**cov_args)["id"]

                # Annual table (the dicionario) gets year-only bounds; the three data
                # tables are month-granular and must carry months on both sides.
                rng_args = dict(
                    coverage_id=cov_id,
                    start_year=start[0],
                    end_year=(end or spec.COVERAGE_END)[0],
                    interval=1,
                    is_closed=is_closed,
                    env=self.env,
                )
                if monthly:
                    rng_args["start_month"] = start[1]
                    rng_args["end_month"] = (end or spec.COVERAGE_END)[1]
                if cov and cov["ranges"]:
                    rng_args["id"] = cov["ranges"][0]["id"]
                server.create_update_datetime_range(**rng_args)
                label = "pro" if is_closed else "free"
                self.log(
                    f"  {slug}: {label} coverage {start} .. {end or spec.COVERAGE_END}"
                )

    # ------------------------------------------------------------- 8. updates
    def updates(self, table_ids: dict[str, str], raw_ids: dict[str, str]):
        current = self.state()["tables"]
        month = self.ids["entity"]["month"]
        today = date.today().isoformat() + "T00:00:00"
        for table in spec.TABLES:
            slug = table["slug"]
            have = [
                u
                for u in current.get(slug, {}).get("updates", [])
                if u["entity_slug"] == "month"
            ]
            args = dict(
                entity_id=month,
                frequency=1,
                lag=1,
                latest=today,
                table_id=table_ids[slug],
                env=self.env,
            )
            if have:
                args["id"] = have[0]["id"]
            if not self.dry_run:
                server.create_update_update(**args)
            self.log(f"  {slug}: table Update latest={today[:10]}")

        # The source-anchored Update carries the source's max COVERAGE date, not a
        # wall clock: it answers "what have they published", which is what the
        # recurring pipeline's poll compares against.
        source_latest = f"{spec.COVERAGE_END[0]:04d}-{spec.COVERAGE_END[1]:02d}-01T00:00:00"
        for key, raw_id in raw_ids.items():
            if self.dry_run:
                self.log(
                    f"  raw source {key}: Update latest={source_latest[:10]}"
                )
                continue
            # Reuse the existing record. create_update_update called without an id
            # CREATES, so omitting this adds a second Update to the same raw source on
            # every re-run -- the exact duplication this script exists to avoid, and one
            # the table-anchored loop above already guards against.
            existing = self.raw_source_update_id(raw_id)
            server.create_update_update(
                entity_id=month,
                frequency=1,
                latest=source_latest,
                raw_data_source_id=raw_id,
                id=existing,
                env=self.env,
            )
            self.log(f"  raw source {key}: Update latest={source_latest[:10]}")

    def raw_source_update_id(self, raw_id: str) -> str | None:
        """The id of this raw source's month-entity Update, or None if it has none."""
        q = """
        query($id: ID!) {
          allRawdatasource(id: $id) { edges { node { updates(first: 10) {
            edges { node { id entity { slug } } }
          } } } }
        }"""
        edges = server._gql(q, {"id": raw_id}, env=self.env)[
            "allRawdatasource"
        ]["edges"]
        if not edges:
            return None
        for upd in edges[0]["node"]["updates"]["edges"]:
            if (upd["node"].get("entity") or {}).get("slug") == "month":
                return server._strip_id(upd["node"]["id"])
        return None

    # --------------------------------------------------------------- 9. order
    def order(self):
        if self.dry_run:
            return
        server.reorder_tables(
            dataset_slug=spec.DATASET_SLUG,
            table_slugs=[t["slug"] for t in spec.TABLES],
            env=self.env,
        )
        self.log("  table order set")

    def run(self, stages: list[str]):
        self.resolve_ids()
        dataset_id = self.dataset()
        self.log("raw sources")
        raw_ids = self.raw_sources(dataset_id)
        self.log("tables")
        table_ids = self.tables(dataset_id, raw_ids)
        if "columns" in stages:
            self.log("columns")
            self.columns(table_ids)
        if "ols" in stages:
            self.log("observation levels")
            self.observation_levels(table_ids)
        if "cloud" in stages:
            self.log("cloud tables")
            self.cloud_tables(table_ids)
        if "coverage" in stages:
            self.log("coverage")
            self.coverage(table_ids)
        if "updates" in stages:
            self.log("updates")
            self.updates(table_ids, raw_ids)
        if "order" in stages:
            self.log("order")
            self.order()


ALL_STAGES = ["columns", "ols", "cloud", "coverage", "updates", "order"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    ap.add_argument("--stages", default=",".join(ALL_STAGES))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--publish",
        action="store_true",
        help="set the dataset status to published instead of under_review",
    )
    args = ap.parse_args()
    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    unknown = [s for s in stages if s not in ALL_STAGES]
    if unknown:
        raise SystemExit(f"unknown stage(s): {unknown}; known: {ALL_STAGES}")
    Registrar(args.env, dry_run=args.dry_run, publish=args.publish).run(stages)
    print("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
