"""Register the us_cms_hcris metadata in the Data Basis backend.

    python register.py --env staging          # dry run is the default
    python register.py --env staging --apply
    python register.py --env prod --apply     # only after the checkpoint

Drives the databasis MCP tools as ordinary Python functions rather than through
the tool interface, so the 114 column payloads never have to be pasted through
a conversation. See [[reference_databasis_mcp_direct_call]].

The write tools are **not idempotent**: ``create_update_observation_level``,
``create_update_cloud_table``, ``create_update_coverage`` and
``create_update_update`` all create a duplicate when re-run without an ``id``.
Every step here therefore reads the current state with ``get_dataset`` first and
passes back the id it finds. Re-running this script is safe.

Column registration is one ``bulk_upsert_columns`` per table — which does write
``bigquery_type``, ``directory_column``, ``measurement_unit``,
``covered_by_dictionary`` and all three languages of description and
observations — followed by one ``update_column`` per identifying column to set
``is_partition`` and to link it to its observation level. ``bulk_upsert_columns``
sets neither, and ``update_column``'s booleans default to False, so the
partition flag is re-passed on every call that touches ``year``.
"""

import argparse
import csv
import json
import sys
from datetime import date
from pathlib import Path

MCP_DIR = Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
sys.path.insert(0, str(MCP_DIR))

import server  # noqa: E402
from dataset_meta import (  # noqa: E402
    AUXILIARY_FILES,
    COVERAGE,
    DATASET_DESCRIPTION,
    DATASET_NAME,
    DATASET_SLUG,
    GCP_DATASET_ID,
    INCOMPLETE_TAIL,
    OBSERVATION_LEVELS,
    ORGANIZATION,
    RAW_SOURCE,
    TABLE_DESCRIPTIONS,
    TABLE_NAMES,
    TAGS_EXISTING,
    TAGS_NEW,
    THEMES,
)
from schema import TABLES  # noqa: E402

CODE_DIR = Path(__file__).resolve().parent
ARCH = CODE_DIR / "architecture"
GCP_PROJECT = {"staging": "basedosdados-dev", "prod": "basedosdados"}
# discover_ids deliberately excludes areas, so the id is looked up by slug.
AREA_SLUG = "us"
# The backend types Update.latest as DateTime, so a bare date is rejected.
TODAY = date.today().isoformat() + "T00:00:00"

# The tables that carry data. dicionario is registered too but has no cloud
# coverage of its own beyond the dataset's.
DATA_TABLES = ["report", "report_value", "hospital_financial"]


class Registrar:
    """Applies the metadata, or prints what it would apply.

    Args:
        env: Backend environment, ``staging`` or ``prod``.
        apply: Write when True; otherwise print the intended calls.
    """

    def __init__(self, env: str, apply: bool) -> None:
        self.env = env
        self.apply = apply
        self.account = (
            server.get_authenticated_account(env=env) if apply else {}
        )

    def call(self, fn: str, **kwargs) -> dict:
        """Invoke one backend tool, or describe it on a dry run.

        Args:
            fn: Name of the ``server`` function.
            **kwargs: Its arguments.

        Returns:
            The tool's result, or an empty dict on a dry run.
        """
        label = ", ".join(
            f"{k}={v!r}"[:80]
            for k, v in kwargs.items()
            if k not in ("columns_json",)
        )
        if not self.apply:
            print(f"  DRY {fn}({label})")
            return {}
        print(f"  {fn}({label})")
        return getattr(server, fn)(env=self.env, **kwargs)

    # -- dataset ---------------------------------------------------------

    def tag_ids(self, ids: dict) -> list[str]:
        """Resolve the dataset's tags, creating the two that do not exist.

        New tags are named here rather than silently invented elsewhere:
        ``medicare`` and ``uncompensated-care``, both English kebab-case slugs
        with lowercase names in all three languages.

        Args:
            ids: The result of ``discover_ids``.

        Returns:
            Tag ids to attach.
        """
        tags = dict(ids.get("tag", {}))
        out = []
        for slug in TAGS_EXISTING:
            if slug not in tags:
                raise SystemExit(f"tag {slug!r} is missing from {self.env}")
            out.append(tags[slug])
        for slug, (pt, en, es) in TAGS_NEW.items():
            if slug in tags:
                out.append(tags[slug])
                continue
            print(f"  creating tag {slug!r} ({en})")
            made = self.call(
                "create_update_tag",
                slug=slug,
                name_pt=pt,
                name_en=en,
                name_es=es,
            )
            out.append(made.get("id"))
        return [t for t in out if t]

    def dataset(self, ids: dict, existing: dict | None) -> str:
        """Create or update the dataset record.

        Registered ``under_review``: that hides it from the production
        frontend, so metadata written before the onboarding PR merges — and
        before the production tables exist — cannot leak publicly.

        Args:
            ids: The result of ``discover_ids``.
            existing: The current dataset, if any.

        Returns:
            The dataset id.
        """
        pt, en, es = DATASET_NAME
        dpt, den, des = DATASET_DESCRIPTION
        res = self.call(
            "create_update_dataset",
            slug=DATASET_SLUG,
            name_pt=pt,
            name_en=en,
            name_es=es,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            organization_ids=[ids["organization"][ORGANIZATION]],
            theme_ids=[ids["theme"][t] for t in THEMES],
            tag_ids=self.tag_ids(ids),
            status_id=ids["status"]["under_review"],
            **({"id": existing["id"]} if existing else {}),
        )
        return (existing or res).get("id", "")


def read_arch(table: str) -> list[dict[str, str]]:
    """Read one architecture CSV.

    Args:
        table: Table slug.

    Returns:
        One dict per column, in architecture order.
    """
    with (ARCH / f"{table}.csv").open() as fh:
        return list(csv.DictReader(fh))


def columns_payload(table: str) -> str:
    """Build the ``bulk_upsert_columns`` payload for one table.

    Args:
        table: Table slug.

    Returns:
        JSON text, one object per column in architecture order.
    """
    payload = []
    for i, c in enumerate(read_arch(table)):
        payload.append(
            {
                "name": c["name"],
                "bigquery_type": c["bigquery_type"],
                "description_pt": c["description_pt"],
                "description_en": c["description_en"],
                "description_es": c["description_es"],
                "observations_pt": c["observations_pt"],
                "observations_en": c["observations_en"],
                "observations_es": c["observations_es"],
                "temporal_coverage": c["temporal_coverage"],
                "covered_by_dictionary": c["covered_by_dictionary"],
                "directory_column": c["directory_column"],
                "measurement_unit": c["measurement_unit"],
                "has_sensitive_data": c["has_sensitive_data"],
                "order": i,
            }
        )
    return json.dumps(payload, ensure_ascii=False)


def column_ids(table_id: str, env: str) -> dict[str, str]:
    """Map column name to backend id for one table.

    ``update_column`` requires the bare column id and no MCP tool returns it,
    so it is read back from GraphQL after ``bulk_upsert_columns`` has created
    the columns.

    Args:
        table_id: Bare table id.
        env: Backend environment.

    Returns:
        ``{column name: column id}``.
    """
    data = server._gql(
        """query($t: ID!) { allColumn(table_Id: $t, first: 500) {
             edges { node { id name } } } }""",
        {"t": table_id},
        env=env,
    )
    return {
        e["node"]["name"]: server._strip_id(e["node"]["id"])
        for e in data["allColumn"]["edges"]
    }


def with_tail(table: str) -> tuple[str, str, str]:
    """Table descriptions, with the incompleteness caveat on the data tables.

    Args:
        table: Table slug.

    Returns:
        The trilingual description.
    """
    base = TABLE_DESCRIPTIONS[table]
    if table not in DATA_TABLES:
        return base
    return tuple(b + t for b, t in zip(base, INCOMPLETE_TAIL, strict=True))


def main() -> None:
    """Register the dataset, its source, tables, columns and coverage."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env", default="staging", choices=["staging", "prod"])
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--tables", nargs="*", default=list(TABLES))
    ap.add_argument(
        "--with-auxiliary-files",
        action="store_true",
        help=(
            "Set auxiliary_files_url on each table. Pass this only once the "
            "bundles are actually in gs://basedosdados/auxiliary_files/, "
            "otherwise the field points at an object that does not exist -- "
            "which is the state seven world_oecd_piaac rows are already in."
        ),
    )
    args = ap.parse_args()

    reg = Registrar(args.env, args.apply)
    # Explicit keys, not the default set: a bare discover_ids() also fetches
    # `allEntityCategory`, which this backend spells `allEntitycategory`, and
    # the whole call fails with HTTP 400. A server-side bug, not one to work
    # around by editing the MCP from a dataset onboarding.
    ids = server.discover_ids(
        env=args.env,
        keys=[
            "status",
            "entity",
            "license",
            "availability",
            "organization",
            "theme",
            "tag",
            "language",
        ],
    )
    area_id = server.lookup_id(slug=AREA_SLUG, category="area", env=args.env)[
        "id"
    ]
    # get_dataset returns a truthy {"found": False, "id": None, ...} stub when
    # the slug is free, so the flag has to be read rather than the dict tested.
    fetched = server.get_dataset(DATASET_SLUG, env=args.env)
    existing = fetched if fetched.get("found") else None
    print(
        f"env={args.env} apply={args.apply} dataset={'found' if existing else 'new'}"
    )

    dataset_id = reg.dataset(ids, existing)
    if not dataset_id:
        print(
            "\ndry run: stopping before the per-table steps, which need a dataset id"
        )
        return

    sources = server.get_raw_data_sources(DATASET_SLUG, env=args.env) or []
    match = next(
        (s for s in sources if s.get("url") == RAW_SOURCE["url"]), None
    )
    pt, en, es = RAW_SOURCE["name"]
    dpt, den, des = RAW_SOURCE["description"]
    source = reg.call(
        "create_update_raw_data_source",
        dataset_id=dataset_id,
        name_pt=pt,
        name_en=en,
        name_es=es,
        description_pt=dpt,
        description_en=den,
        description_es=des,
        url=RAW_SOURCE["url"],
        availability_id=ids["availability"]["online"],
        license_id=ids["license"]["cc0"],
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        language_ids=[ids["language"]["en"]],
        **({"id": match["id"]} if match else {}),
    )
    source_id = (match or source).get("id", "")

    account = reg.account.get("id", "")

    def link_raw_source(table, table_id, tpt, ten, tes, dpt, den, des) -> None:
        """Link the table to the raw data source, deferred until it exists.

        ``CreateUpdateTable`` is a full replace rather than a patch, so every
        field set on the first pass is repeated here; omitting one clears it.
        """
        if not source_id:
            return
        reg.call(
            "create_update_table",
            id=table_id,
            slug=table,
            dataset_id=dataset_id,
            name_pt=tpt,
            name_en=ten,
            name_es=tes,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            status_id=ids["status"]["published"],
            published_by_ids=[account] if account else [],
            data_cleaned_by_ids=[account] if account else [],
            raw_data_source_ids=[source_id],
            **(
                {"auxiliary_files_url": AUXILIARY_FILES[table]}
                if args.with_auxiliary_files and table in AUXILIARY_FILES
                else {}
            ),
        )

    for table in args.tables:
        print(f"\n== {table}")
        # get_dataset returns `tables` as a dict keyed by slug, not a list.
        found = (existing or {}).get("tables", {}).get(table)
        tpt, ten, tes = TABLE_NAMES[table]
        dpt, den, des = with_tail(table)
        res = reg.call(
            "create_update_table",
            slug=table,
            dataset_id=dataset_id,
            name_pt=tpt,
            name_en=ten,
            name_es=tes,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            status_id=ids["status"]["published"],
            published_by_ids=[account] if account else None,
            data_cleaned_by_ids=[account] if account else None,
            **(
                {"auxiliary_files_url": AUXILIARY_FILES[table]}
                if args.with_auxiliary_files and table in AUXILIARY_FILES
                else {}
            ),
            **({"id": found["id"]} if found else {}),
        )
        table_id = (found or res).get("id", "")
        if not table_id:
            continue

        level_ids = {}
        for entity, _ in OBSERVATION_LEVELS.get(table, []):
            prior = next(
                (
                    ol
                    for ol in (found or {}).get("observation_levels", [])
                    if ol.get("entity_slug") == entity
                ),
                None,
            )
            made = reg.call(
                "create_update_observation_level",
                table_id=table_id,
                entity_id=ids["entity"][entity],
                **({"id": prior["id"]} if prior else {}),
            )
            level_ids[entity] = (prior or made).get("id", "")

        reg.call(
            "bulk_upsert_columns",
            table_id=table_id,
            columns_json=columns_payload(table),
        )

        # update_column needs the bare column id, which no MCP tool returns.
        by_name = column_ids(table_id, args.env) if reg.apply else {}

        # bulk_upsert_columns sets neither is_partition nor the observation
        # level link, and update_column's booleans default to False -- so the
        # partition flag is re-passed on the call that links year.
        for entity, column in OBSERVATION_LEVELS.get(table, []):
            if reg.apply and column not in by_name:
                raise SystemExit(
                    f"{table}.{column} was not created by bulk_upsert"
                )
            reg.call(
                "update_column",
                column_id=by_name.get(column, ""),
                column_name=column,
                table_id=table_id,
                observation_level_id=level_ids.get(entity),
                is_partition=(column == "year"),
            )

        prior_cloud = next(iter((found or {}).get("cloud_tables", [])), None)
        reg.call(
            "create_update_cloud_table",
            table_id=table_id,
            gcp_project_id=GCP_PROJECT[args.env],
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            **({"id": prior_cloud["id"]} if prior_cloud else {}),
        )
        link_raw_source(table, table_id, tpt, ten, tes, dpt, den, des)
        if table not in DATA_TABLES:
            # dicionario has no date column, so no coverage and no update.
            continue

        prior_cov = next(
            (
                c
                for c in (found or {}).get("coverages", [])
                if c.get("area_slug") == AREA_SLUG
            ),
            None,
        )
        coverage = reg.call(
            "create_update_coverage",
            table_id=table_id,
            area_id=area_id,
            **({"id": prior_cov["id"]} if prior_cov else {}),
        )
        coverage_id = (prior_cov or coverage).get("id", "")
        if coverage_id:
            prior_range = next(
                iter((prior_cov or {}).get("datetime_ranges", [])), None
            )
            reg.call(
                "create_update_datetime_range",
                coverage_id=coverage_id,
                start_year=COVERAGE["start_year"],
                end_year=COVERAGE["end_year"],
                interval=1,
                **({"id": prior_range["id"]} if prior_range else {}),
            )
        # Table-anchored Update: when Data Basis last refreshed the table -- a
        # wall clock, not a date read from the data. CMS reissues every archive
        # quarterly, so the cadence is three months. The recurring flow rewrites
        # `latest` on every materialization.
        prior_upd = next(iter((found or {}).get("updates", [])), None)
        reg.call(
            "create_update_update",
            table_id=table_id,
            entity_id=ids["entity"]["month"],
            frequency=3,
            lag=0,
            latest=TODAY,
            **({"id": prior_upd["id"]} if prior_upd else {}),
        )

    print("\ndone")


if __name__ == "__main__":
    main()
