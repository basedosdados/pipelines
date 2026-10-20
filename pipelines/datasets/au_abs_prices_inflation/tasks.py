"""Tasks for au_abs_prices_inflation — Prefect 3.

7 independent check_update/extract_and_load pairs, one per table
(one-pipeline-per-table_id is the repo-wide rule, see
`pipelines.utils.stage_dispatch.pipeline_factory`): `cpi_quarterly`,
`cpi_monthly`, and the five other ABS releases (`wage_price_index`,
`producer_price_index`, `international_trade_price_index`,
`living_cost_index`, `dwelling_value`).

ABS exposes no cheap "has this changed" signal (no Last-Modified worth
trusting, no version header) -- the only way to know the latest published
period is to download and parse the release's workbooks. So, same as every
other staged-pipeline dataset, `get_latest_update` downloads and parses
whatever `extract_load_data` would need to decide the reference date, and
`extract_load_data` downloads and parses it again, independently, when
dispatched. Duplicate download cost, accepted in exchange for the uniform
check_update -> extract_and_load shape.

The two CPI tables share one ABS release (and `SOURCE_TABLES` already lists
their source workbooks disjointly), so splitting them one-per-table costs no
extra download: each downloads only the workbooks its own frequency needs
(`cpi.download_frequency`). The other five releases were already one-per-table
(`timeseries.download_release` takes no other release's files).

Every ABS release ships the full history in its workbooks, so every table is a
full replace each run -- `ExtractAndLoad`'s default `dump_mode="append"` is
enough: `upload_to_gcs` replaces each partition's blob wholesale
(`st.upload(..., if_exists="replace")`), and since every run writes the same
partition file names (`year=YYYY/data.parquet`) for the full history, the
semantics are the same as a real overwrite, without the production-table
delete `dump_mode="overwrite"` would trigger (confirmed on us_sec_edgar,
2026-08-19 -- see `pipelines.utils.tasks.upload_to_gcs`).
"""

import datetime as dt
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path

from pipelines.datasets.au_abs_prices_inflation.constants import (
    CPI_COVERAGE,
    CPI_MONTHLY_TABLE_ID,
    CPI_QUARTERLY_TABLE_ID,
    RELEASE_COVERAGE,
    constants,
)
from pipelines.datasets.au_abs_prices_inflation.cpi import (
    clean_frequency,
    download_frequency,
)
from pipelines.datasets.au_abs_prices_inflation.cpi import (
    max_year_month as cpi_max_year_month,
)
from pipelines.datasets.au_abs_prices_inflation.cpi import (
    write_partitioned as write_cpi_partitioned,
)
from pipelines.datasets.au_abs_prices_inflation.releases import build_release
from pipelines.datasets.au_abs_prices_inflation.releases import (
    max_year_month as release_max_year_month,
)
from pipelines.datasets.au_abs_prices_inflation.releases import (
    write_partitioned as write_release_partitioned,
)
from pipelines.datasets.au_abs_prices_inflation.timeseries import (
    download_release,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

DATASET_ID = constants.DATASET_ID.value

# table_id (`cpi_quarterly`/`cpi_monthly`) -> frequency key used by
# `cpi.download_frequency`/`cpi.clean_frequency` (`"quarterly"`/`"monthly"`).
_CPI_FREQUENCY = {
    CPI_QUARTERLY_TABLE_ID: "quarterly",
    CPI_MONTHLY_TABLE_ID: "monthly",
}


# ──────────────────────────────────────────────────────────────────────────────
# cpi_quarterly / cpi_monthly
# ──────────────────────────────────────────────────────────────────────────────


def make_cpi_get_latest_update(
    table_id: str,
) -> Callable[[], SourceInspection]:
    """Build `table_id`'s check_update: download + parse, report its latest period.

    No parquet is written here -- only `clean_frequency`'s in-memory
    DataFrame is needed to read off the latest period.
    """
    frequency = _CPI_FREQUENCY[table_id]

    def get_latest_update() -> SourceInspection:
        work_dir = tempfile.mkdtemp(
            prefix=f"au_abs_prices_inflation_{table_id}_check_"
        )
        try:
            download_frequency(frequency, work_dir)
            df = clean_frequency(frequency, work_dir)
            return SourceInspection(
                reference_date=dt.datetime.strptime(
                    cpi_max_year_month(df, frequency), "%Y-%m"
                ).date()
            )
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    return get_latest_update


def make_cpi_extract_load_data(
    table_id: str,
) -> Callable[[dict], ExtractAndLoad]:
    """Build `table_id`'s extract_and_load: download, clean, write parquet."""
    frequency = _CPI_FREQUENCY[table_id]

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Download + clean the current CPI release, keeping only `table_id`.

        Args:
            download_params: `{"reference_date": "<latest period found at check
                time>"}` -- unused: ABS always serves the current release, so
                there is nothing to round-trip through this stage. Kept for
                interface symmetry with `check_update_and_dispatch`, which
                always passes `reference_date` (same trade-off as
                `au_ato_abr.tasks.make_extract_load_data`).

        Returns:
            `ExtractAndLoad` for `table_id`. Not cleaned up afterwards (no
            `shutil.rmtree`): `data_path` points inside `work_dir` and is only
            read, via `upload_to_gcs`, right after this returns, inside the
            same flow run -- `CheckThenExtractLoadPipeline.run_extract_and_load`
            uploads immediately, then the flow (and its worker) ends.
        """
        work_dir = tempfile.mkdtemp(
            prefix=f"au_abs_prices_inflation_{table_id}_"
        )
        input_dir = Path(work_dir) / "input"
        output_dir = Path(work_dir) / "output"
        download_frequency(frequency, str(input_dir))
        df = clean_frequency(frequency, str(input_dir))
        write_cpi_partitioned(df, table_id, str(output_dir))

        return ExtractAndLoad(
            coverage=CPI_COVERAGE[table_id].model_dump(),
            data_path=str(output_dir / table_id),
            source_format="parquet",
        )

    return extract_load_data


make_cpi_pipeline = pipeline_factory(
    DATASET_ID,
    make_cpi_get_latest_update,
    make_cpi_extract_load_data,
    # ABS reports CPI periods as "YYYY-MM" -- see `cpi.max_year_month`.
    date_format="%Y-%m",
)


# ──────────────────────────────────────────────────────────────────────────────
# wage_price_index / producer_price_index / international_trade_price_index /
# living_cost_index / dwelling_value
# ──────────────────────────────────────────────────────────────────────────────


def make_release_get_latest_update(
    release: str,
) -> Callable[[], SourceInspection]:
    """Build `release`'s check_update: download + parse, report its latest period.

    No parquet is written here -- only `build_release`'s in-memory DataFrame is
    needed to read off the latest period.
    """

    def get_latest_update() -> SourceInspection:
        work_dir = tempfile.mkdtemp(
            prefix=f"au_abs_prices_inflation_{release}_check_"
        )
        try:
            download_release(release, work_dir)
            df = build_release(release, work_dir)
            return SourceInspection(
                reference_date=dt.datetime.strptime(
                    release_max_year_month(df), "%Y-%m"
                ).date()
            )
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    return get_latest_update


def make_release_extract_load_data(
    release: str,
) -> Callable[[dict], ExtractAndLoad]:
    """Build `release`'s extract_and_load: download, clean, write parquet."""

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Download + clean the current ABS release.

        Args:
            download_params: `{"reference_date": "<latest period found at
                check time>"}` -- unused, same reasoning as
                `make_cpi_extract_load_data.extract_load_data`.

        Returns:
            `ExtractAndLoad` for `release`. Not cleaned up afterwards -- same
            trade-off as `make_cpi_extract_load_data.extract_load_data`.
        """
        work_dir = tempfile.mkdtemp(
            prefix=f"au_abs_prices_inflation_{release}_"
        )
        input_dir = Path(work_dir) / "input"
        output_dir = Path(work_dir) / "output"
        download_release(release, str(input_dir))
        df = build_release(release, str(input_dir))
        write_release_partitioned(df, release, str(output_dir))

        return ExtractAndLoad(
            coverage=RELEASE_COVERAGE.model_dump(),
            data_path=str(output_dir / release),
            source_format="parquet",
        )

    return extract_load_data


make_release_pipeline = pipeline_factory(
    DATASET_ID,
    make_release_get_latest_update,
    make_release_extract_load_data,
    # The poll compares a "%Y-%m" string against the registered coverage --
    # see `releases.max_year_month`.
    date_format="%Y-%m",
)
