"""Constants for the au_geoscape_gnaf recurring pipeline (Prefect 3).

Geoscape **G-NAF** (Geocoded National Address File, data.gov.au). The source
republishes a full snapshot **quarterly** (Feb/May/Aug/Nov); we stack each
release, partitioned by ``snapshot_date`` (CNPJ model — see
``models/au_geoscape_gnaf/code/DESIGN.md``). Each run uploads the new snapshot to
staging with ``dump_mode="overwrite"`` and the **incremental** dbt models append
its ``snapshot_date`` partition to the prod tables, so history accumulates.

The download URL carries a per-release resource UUID and build token
(``.../resource/<uuid>/download/g-naf_<mon><yy>_allstates_gda2020_psv_<n>.zip``)
that change every quarter, so the pipeline resolves the current resource from the
CKAN ``package_show`` API at run time rather than hard-coding a URL.
"""

from enum import Enum
from pathlib import Path

from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    NonHistorical,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the au_geoscape_gnaf pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/au_geoscape_gnaf/code/`` (the schema source of truth for both this
    pipeline and the one-shot bootstrap).
    """

    DATASET_ID = "au_geoscape_gnaf"

    # data.gov.au 403s automated clients without a browser User-Agent.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )

    # CKAN package for G-NAF. package_show lists every release resource; the
    # pipeline picks the current GDA2020 all-states PSV zip (see
    # utils.resolve_source). The package id is stable across releases.
    CKAN_PACKAGE = "geocoded-national-address-file-g-naf"
    CKAN_PACKAGE_SHOW = (
        "https://data.gov.au/data/api/3/action/package_show"
        "?id=geocoded-national-address-file-g-naf"
    )

    # The core (poll) table + the full table list. address_detail is the primary
    # table the source-update poll is anchored on.
    CORE_TABLE = "address_detail"
    DATA_TABLES = ["address_detail", "street_locality", "locality"]
    ALL_TABLES = [
        "address_detail",
        "street_locality",
        "locality",
        "dicionario",
    ]

    # State/territory abbreviations the release unpacks into (per-state PSVs).
    ALL_STATES = ["ACT", "NSW", "NT", "OT", "QLD", "SA", "TAS", "VIC", "WA"]

    # Release month abbreviation (in the zip filename) -> month number. G-NAF
    # ships Feb/May/Aug/Nov, but the full map keeps snapshot_date derivation
    # robust to an off-cycle release.
    MONTHS = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "au_geoscape_gnaf" / "code" / "architecture"
    )


# Coverage spec per table, used by `ExtractAndLoad.coverage` (tasks.py) and,
# through it, by `register_table_materialization_task`/`build_and_promote`.
#
# G-NAF is CC-BY / Open-G-NAF, so every dated table is AllFree — the whole
# stacked history is public. `snapshot_date` (a DATE) is the coverage column,
# shared by the 3 data tables (same instance, read-only). `dicionario` has no
# date column, so it takes `NonHistorical` instead — modeled on
# `__TABLES__.last_modified_time`, not a `DateColumn`/`DateFormat` pair. The
# old monolithic flow never registered any coverage for `dicionario` at all;
# this is its first baseline, not a change to a previously decided value.
_ALL_FREE = AllFree(
    date_column=DateOnly(col="snapshot_date"),
    date_format=DateFormat.YEAR_MD,
)
COVERAGE = {
    "address_detail": _ALL_FREE,
    "street_locality": _ALL_FREE,
    "locality": _ALL_FREE,
    "dicionario": NonHistorical(),
}
