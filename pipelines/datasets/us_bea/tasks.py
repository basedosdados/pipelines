"""Prefect 3 tasks for us_bea — thin wrappers over utils.py, plus the
make_get_latest_update/make_extract_load_data factories dispatched through
`CheckThenExtractLoadPipeline` (see the banner above the factories below).
"""

import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.us_bea.constants import COVERAGE
from pipelines.datasets.us_bea.utils import (
    gi_latest_year_quarter,
    nipa_latest_year_month,
    regional_latest,
    write_dicionario,
    write_gi,
    write_nipa,
    write_regional,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# --------------------------------------------------------------------------- #
# @task wrappers — one reference-date task and one write task per table
# family, all calling straight into utils.py. The BEA REST API is per-series,
# not a bulk archive: there is nothing to download once and slice across
# tables, so each fetch below is already fully independent of the others. The
# only *repeated* work is a table's own check_update re-fetching what its own
# extract_and_load fetches again a moment later — the accepted repo-wide cost
# of a check_update that needs to download data to find a reference date (see
# the module banner in flows.py).
# --------------------------------------------------------------------------- #


@task(retries=2, retry_delay_seconds=30)
def nipa_reference_task() -> tuple[int, int] | None:
    """Fetch every NIPA table; return the latest (year, month) among monthly rows."""
    return nipa_latest_year_month()


@task
def write_nipa_task(work_dir: str) -> str:
    """Fetch + write the nipa table; see :func:`utils.write_nipa`."""
    return str(write_nipa(Path(work_dir) / "output"))


@task(retries=2, retry_delay_seconds=30)
def gi_reference_task() -> tuple[int, int] | None:
    """Fetch every GDPbyIndustry table; return the latest (year, quarter)."""
    return gi_latest_year_quarter()


@task
def write_gi_task(work_dir: str) -> str:
    """Fetch + write the gdp_by_industry table; see :func:`utils.write_gi`."""
    return str(write_gi(Path(work_dir) / "output"))


@task(retries=2, retry_delay_seconds=30)
def regional_reference_task(
    table_id: str,
) -> tuple[int | None, tuple[int, int] | None]:
    """Fetch one regional BEA table family; see :func:`utils.regional_latest`."""
    return regional_latest(table_id)


@task
def write_regional_task(table_id: str, work_dir: str) -> str:
    """Fetch + write one regional BEA table; see :func:`utils.write_regional`."""
    return str(write_regional(table_id, Path(work_dir) / "output"))


@task
def write_dicionario_task(work_dir: str) -> str:
    """Build + write the dicionario table; see :func:`utils.write_dicionario`."""
    return str(write_dicionario(Path(work_dir) / "output"))


# --------------------------------------------------------------------------- #
# make_get_latest_update / make_extract_load_data
#
# One pipeline per table_id — the repo-wide default shape (see
# `pipeline_factory` in stage_dispatch.py, and `br_ibge_ipca` for the
# reference layout). Each BEA "dataset" family (NIPA / GDPbyIndustry /
# Regional) already has its own independent GetData calls, so — unlike
# au_geoscape_gnaf's single release zip feeding 4 tables — there is no shared
# download being wastefully repeated here: fetching nipa never touches
# gdp_by_industry's or regional_state's rows. `models/us_bea/schema.yml` also
# has no `ref()` between the six us_bea models, so there is no dbt
# test-ordering reason for a batched extract_and_load either (contrast
# br_bcb_ifdata/au_rba_statistical_tables, which keep a single pair for the
# whole dataset for exactly that reason).
#
# `dicionario` has no date column (`NonHistorical`, see constants.py) and its
# own content (BEA table/industry code -> label maps) carries no date signal
# at all — it is built from cheap `GetParameterValues` listings, never
# `GetData` (see `write_dicionario`). Its check_update reuses nipa's monthly
# reference as a freshness proxy (same reasoning as au_geoscape_gnaf's
# dicionario, which reuses the dataset's resolved release signal): new
# table/industry codes tend to surface alongside a new NIPA release, and nipa
# is already the dataset's highest-frequency, most-current table.
# `compare_against="table_update"` reads `Table.Update.latest` instead of the
# default `Coverage.DateTimeRange`, since `NonHistorical` has no coverage
# baseline to compare a reference date against.
# --------------------------------------------------------------------------- #


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Build the check_update callable for one table.

    Args:
        table_id: one of `constants.ALL_TABLES`.

    Returns:
        A zero-arg callable returning a `SourceInspection` with the latest
        reference date found at the source for this table.

    Raises:
        RuntimeError: if the relevant BEA call returns no rows at all (an
            empty source, not "no new data" — there is nothing to compare).
        ValueError: if `table_id` isn't one of the six us_bea tables.
    """

    def get_latest_update() -> SourceInspection:
        if table_id in ("nipa", "dicionario"):
            max_ym = nipa_reference_task()
            if max_ym is None:
                raise RuntimeError(
                    "us_bea: no monthly NIPA rows found at the source"
                )
            year, month = max_ym
            return SourceInspection(
                reference_date=date(year, month, 1),
                compare_against=(
                    "table_update" if table_id == "dicionario" else "coverage"
                ),
            )

        if table_id == "gdp_by_industry":
            max_yq = gi_reference_task()
            if max_yq is None:
                raise RuntimeError(
                    "us_bea: no GDPbyIndustry rows found at the source"
                )
            year, quarter = max_yq
            return SourceInspection(reference_date=date(year, quarter * 3, 1))

        if table_id == "regional_state":
            _, max_yq = regional_reference_task(table_id)
            if max_yq is None:
                raise RuntimeError(
                    "us_bea: no regional_state rows found at the source"
                )
            year, quarter = max_yq
            return SourceInspection(reference_date=date(year, quarter * 3, 1))

        if table_id in ("regional_county", "regional_metro"):
            max_year, _ = regional_reference_task(table_id)
            if max_year is None:
                raise RuntimeError(
                    f"us_bea: no {table_id} rows found at the source"
                )
            return SourceInspection(reference_date=date(max_year, 1, 1))

        raise ValueError(f"us_bea: unknown table_id {table_id!r}")

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Build the extract_and_load callable for one table.

    Args:
        table_id: one of `constants.ALL_TABLES`.

    Returns:
        A callable that re-fetches this table's data from the BEA API
        (independently of `get_latest_update`, see the module banner above)
        and returns its `ExtractAndLoad`. BEA benchmark revisions rewrite
        history, so every run is a full replace (`dump_mode="overwrite"`),
        never an incremental append.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Not cleaned up here: each flow run is its own throwaway worker
        # container, same convention as au_geoscape_gnaf's
        # `make_extract_load_data` — the caller
        # (`CheckThenExtractLoadPipeline.run_extract_and_load`) reads
        # `data_path` via `upload_to_gcs` after this function returns.
        work_dir = tempfile.mkdtemp(prefix=f"us_bea_{table_id}_")

        if table_id == "nipa":
            data_path = write_nipa_task(work_dir=work_dir)
        elif table_id == "gdp_by_industry":
            data_path = write_gi_task(work_dir=work_dir)
        elif table_id in (
            "regional_state",
            "regional_county",
            "regional_metro",
        ):
            data_path = write_regional_task(
                table_id=table_id, work_dir=work_dir
            )
        elif table_id == "dicionario":
            data_path = write_dicionario_task(work_dir=work_dir)
        else:
            raise ValueError(f"us_bea: unknown table_id {table_id!r}")

        return ExtractAndLoad(
            coverage=COVERAGE[table_id].model_dump(),
            data_path=data_path,
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data
