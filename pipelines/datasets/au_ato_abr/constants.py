"""Constants for the au_ato_abr recurring pipeline (Prefect 3).

Australian Business Register "ABN Bulk Extract" (data.gov.au, ATO). The source
republishes a full snapshot **weekly**; we stack each snapshot, partitioned by
``extraction_date`` (see models/au_ato_abr/ONBOARDING_PLAN.md). Each run uploads
the new snapshot to staging with ``dump_mode="overwrite"`` and the incremental
dbt models append the new ``extraction_date`` partition to the prod tables.

One independent check_update/extract_and_load pair per table (entity,
other_name, dgr, dicionario) — the usual one-pipeline-per-table_id shape (see
``pipelines.utils.stage_dispatch.pipeline_factory``). Each table's
extract_and_load redownloads the two source ZIPs and reruns the whole
streaming ``lxml.iterparse`` pass from scratch, keeping only its own table
(see the banner in ``tasks.py``) — redundant, since a single pass already
builds all 4 tables together (``utils.clean_all``), but uniform 1-flow-per-
table is the rule now; ``br_mf_divida_ativa`` took the same trade-off for a
similar shared-download, multi-table source.
"""

from enum import Enum
from pathlib import Path

from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    NonHistorical,
    PartBdpro,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the au_ato_abr pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/au_ato_abr/code/`` (the schema source of truth for both this
    pipeline and the one-shot bootstrap).
    """

    DATASET_ID = "au_ato_abr"

    # data.gov.au 403s automated clients without a browser User-Agent.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )

    # CKAN package: the two split ZIPs are re-published in place each week (same
    # resource ids, new files). If the source ever changes the resource ids,
    # update these two URLs.
    CKAN_PACKAGE = "abn-bulk-extract"
    CKAN_PACKAGE_SHOW = "https://data.gov.au/data/api/3/action/package_show?id=abn-bulk-extract"
    ZIP_URLS = [
        "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/"
        "resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip",
        "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/"
        "resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip",
    ]

    # Tables (partitioned parquet) + the static dictionary.
    DATA_TABLES = ["entity", "other_name", "dgr"]
    ALL_TABLES = ["entity", "other_name", "dgr", "dicionario"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "au_ato_abr" / "code" / "architecture"
    )


# Table ids. Kept here as plain literals (not Enum members) so `tasks.py`/
# `flows.py` can import them directly — one per independent check_update/
# extract_and_load pair (see the module banner).
ENTITY_TABLE_ID = "entity"
OTHER_NAME_TABLE_ID = "other_name"
DGR_TABLE_ID = "dgr"
DICIONARIO_TABLE_ID = "dicionario"

# Coverage spec per table, used by `ExtractAndLoad.coverage` (tasks.py) and,
# through it, by `register_table_materialization_task`/`build_and_promote`.
#
# The register refreshes weekly, so the three data tables carry the BD Pro
# rolling window: the most recent `free_lag` of snapshots are pro-only, older
# snapshots stay free. Each run recomputes free_end = source_end - free_lag,
# rewrites both DateTimeRanges, and re-issues the BigQuery Row Access Policies,
# so the window slides forward on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. The static onboard registered only the free
# Coverage, so the pro Coverage must be created on each of entity/other_name/dgr
# BEFORE this flow is armed (see the pipeline PR notes / ONBOARDING_PLAN.md).
#
# free_lag is a business choice: with weekly snapshots and a full register per
# snapshot, the free tier always holds a complete (if lagged) register. 6 months
# mirrors br_rf_cnpj; a shorter lag (e.g. FreeLag("weeks", 4)) narrows the
# initial free-tier lockout at arm time. Confirm before arming.
#
# `dicionario` has no date column, so it takes `NonHistorical` instead —
# modeled on `__TABLES__.last_modified_time`, not a `DateColumn`/`DateFormat`
# pair (same pattern as au_geoscape_gnaf's constants.py).
_PART_BDPRO = PartBdpro(
    date_column=DateOnly(col="extraction_date"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="months", value=6),
)
COVERAGE = {
    ENTITY_TABLE_ID: _PART_BDPRO,
    OTHER_NAME_TABLE_ID: _PART_BDPRO,
    DGR_TABLE_ID: _PART_BDPRO,
    DICIONARIO_TABLE_ID: NonHistorical(),
}
