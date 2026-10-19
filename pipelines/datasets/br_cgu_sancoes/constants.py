"""Constants for the br_cgu_sancoes recurring pipeline (Prefect 3).

CGU sanctions registries published by the Portal da Transparência: CEIS, CNEP,
CEPIM and Acordos de Leniência (which ships two CSVs, Acordos + Efeitos). Each
registry is an on-demand live snapshot behind AWS WAF: requesting
``/download-de-dados/<registry>/<YYYYMMDD>`` triggers generation and 302-redirects
to the S3 zip once ready. There is no historical archive — only the current
snapshot is retrievable — so every run fetches the latest available snapshot and
overwrites (``dump_mode="overwrite"``), stamping ``data_extracao`` with the
snapshot date.

The cleaning transform and column schema live in ``utils.py`` (the schema source
of truth for this dataset, since there are no architecture CSVs); the one-shot
bootstrap in ``models/br_cgu_sancoes/code/clean.py`` imports the same functions.

One independent check_update/extract_and_load pair per table (the usual
one-pipeline-per-table_id shape, see ``pipelines.utils.stage_dispatch.
pipeline_factory``). ``ceis``/``cnep``/``cepim`` each own an independent portal
registry; ``acordos_leniencia``/``acordos_leniencia_efeitos`` share the single
``acordos-leniencia`` zip (two CSVs from one download) — each table's
extract_and_load still redownloads and reparses it independently rather than
sharing a pass, same trade-off ``au_ato_abr``/``au_geoscape_gnaf`` took for a
similar shared-download, multi-table source: uniform 1-flow-per-table now
outweighs the saved bandwidth. ``dicionario`` is authored directly in Python
(``utils.DICIONARIO_ROWS``, no download at all) and has no registry of its
own, so its check_update borrows ``ceis``'s registry purely as a real,
non-fabricated freshness signal (``dicionario`` decodes ``tipo_pessoa``, a
column ``ceis``/``cnep`` both have) — see ``REGISTRY_FOR_TABLE``/
``DICIONARIO_CHECK_REGISTRY`` and ``tasks.make_get_latest_update``.

No real cross-table dbt dependency exists between the six tables: every model
in ``models/br_cgu_sancoes/*.sql`` selects straight from its own
``<table>_staging`` source with no ``ref()``/join to a sibling table, and every
``schema.yml`` test (``unique_combination_of_columns``,
``not_null_proportion_multiple_columns``) is scoped to its own table's columns.
So the repo-wide "1 flow per table" default applies cleanly here, with no
``dispatch_build_and_promote``-bypassing group flow needed.
"""

from enum import Enum

from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    FreeLag,
    NonHistorical,
    PartBdpro,
)


class constants(Enum):
    """Constants for the br_cgu_sancoes pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "br_cgu_sancoes"

    # The portal blocks non-browser clients (405) and the S3 layer returns a
    # bare 403 for a not-yet-generated / stale date. A browser User-Agent is
    # mandatory; reuse the same string the sibling pipelines/shared/cgu use.
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    # On-demand download entrypoint. Requesting it for a date generates the zip
    # (202 while preparing, 200 via redirect when ready). {registry} is the
    # portal path segment; {date} is YYYYMMDD.
    PORTAL_URL = "https://portaldatransparencia.gov.br/download-de-dados/{registry}/{date}"

    # Portal registry path -> the output table slug(s) it produces. The Acordos
    # de Leniência zip ships two CSVs, so its one download feeds two tables.
    REGISTRIES = {
        "ceis": ["ceis"],
        "cnep": ["cnep"],
        "cepim": ["cepim"],
        "acordos-leniencia": [
            "acordos_leniencia",
            "acordos_leniencia_efeitos",
        ],
    }

    # Table slug -> the raw CSV filename suffix (after the "YYYYMMDD_" prefix)
    # that the registry zip extracts to. Used to pick the right CSV per table.
    FILE_SUFFIX = {
        "ceis": "CEIS.csv",
        "cnep": "CNEP.csv",
        "cepim": "CEPIM.csv",
        "acordos_leniencia": "Acordos.csv",
        "acordos_leniencia_efeitos": "Efeitos.csv",
    }

    # Data tables (partitioned parquet) + the static dictionary.
    DATA_TABLES = [
        "ceis",
        "cnep",
        "cepim",
        "acordos_leniencia",
        "acordos_leniencia_efeitos",
    ]
    ALL_TABLES = [
        "ceis",
        "cnep",
        "cepim",
        "acordos_leniencia",
        "acordos_leniencia_efeitos",
        "dicionario",
    ]

    # How many days back to probe for the latest ready snapshot before giving up.
    MAX_LOOKBACK_DAYS = 8
    # Seconds to wait between 202 ("preparing") retries, and how many times.
    POLL_WAIT_SECONDS = 30
    POLL_MAX_RETRIES = 8


# Table ids. Kept here as plain literals (not Enum members) so `tasks.py`/
# `flows.py` can import them directly — one per independent check_update/
# extract_and_load pair (see the module banner).
CEIS_TABLE_ID = "ceis"
CNEP_TABLE_ID = "cnep"
CEPIM_TABLE_ID = "cepim"
ACORDOS_LENIENCIA_TABLE_ID = "acordos_leniencia"
ACORDOS_LENIENCIA_EFEITOS_TABLE_ID = "acordos_leniencia_efeitos"
DICIONARIO_TABLE_ID = "dicionario"

# table_id -> portal registry key (see `constants.REGISTRIES` above for the
# registry -> table_id(s) it produces). `acordos_leniencia`/
# `acordos_leniencia_efeitos` share one registry — see the module banner.
# `dicionario` is deliberately absent: it has no registry of its own, see
# `DICIONARIO_CHECK_REGISTRY` below.
REGISTRY_FOR_TABLE = {
    CEIS_TABLE_ID: "ceis",
    CNEP_TABLE_ID: "cnep",
    CEPIM_TABLE_ID: "cepim",
    ACORDOS_LENIENCIA_TABLE_ID: "acordos-leniencia",
    ACORDOS_LENIENCIA_EFEITOS_TABLE_ID: "acordos-leniencia",
}

# `dicionario` borrows this registry's snapshot date as its check_update signal
# (no registry/date of its own — see the module banner). Picked over cnep
# arbitrarily; either would do since both carry `tipo_pessoa`.
DICIONARIO_CHECK_REGISTRY = "ceis"

# Coverage spec per table, used by `ExtractAndLoad.coverage` (tasks.py) and,
# through it, by `register_table_materialization_task`/`build_and_promote`.
#
# ceis / cnep / acordos_leniencia are the high-frequency compliance tables and
# carry the BD Pro rolling window, keyed on the SANCTION START date — a sanction
# that started in the last 6 months is pro-only, everything older is free. Each
# run recomputes free_end = max(<start_date>) - 6 months, rewrites both
# DateTimeRanges and re-issues the BigQuery Row Access Policies, so the window
# slides on its own. The paywall columns are 0% null, so the standard RAP
# (<date_col> <= free_end) hides nothing unintentionally.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written — these three are onboarded AllFree, so the pro
# Coverage must be created by hand before the first armed run.
#
# cepim / acordos_leniencia_efeitos have no content date; their only date column
# is the snapshot stamp, so they stay fully free keyed on data_extracao.
#
# `dicionario` has no date column, so it takes `NonHistorical` instead — same
# pattern as `au_ato_abr`/`au_geoscape_gnaf`. The old monolithic flow never
# registered any coverage for `dicionario` at all; this is its first baseline,
# not a change to a previously decided value.
_PART_BDPRO_6M = FreeLag(unit="months", value=6)
COVERAGE = {
    CEIS_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_inicio_sancao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
    CNEP_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_inicio_sancao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
    ACORDOS_LENIENCIA_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_inicio_acordo"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
    CEPIM_TABLE_ID: AllFree(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    ),
    ACORDOS_LENIENCIA_EFEITOS_TABLE_ID: AllFree(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    ),
    DICIONARIO_TABLE_ID: NonHistorical(),
}
