"""Pure-function tests for the br_cgu_sancoes BD Pro rolling window.

The paywall for ceis/cnep/acordos_leniencia is keyed on the *sanction start*
date (a content column, ``data_inicio_sancao`` / ``data_inicio_acordo``), not on
the snapshot/release date. These tests confirm the pure metadata policy accepts
that arbitrary date column and that the window rolls as the source advances.
No network, no BigQuery — ``apply_row_access_policies`` (the actual DDL) needs
the worker and is not exercised here.

The specs here are built by hand (`CoverageSpec`/`CoverageColumn`, see
`pipelines.utils.metadata.coverage`) instead of imported from
`constants.py` — there is no local `COVERAGE` constant anymore, it comes from
the backend (`coverage_from_backend`) at run time. `free_lag` is no longer a
per-spec field either — `policy.PART_BDPRO_FREE_LAG` is a fixed 6-month
constant shared by every dataset.
"""

from datetime import date

import pytest

from pipelines.utils.metadata.coverage import (
    CoverageColumn,
    CoverageSpec,
    Tier,
    date_format_for,
)
from pipelines.utils.metadata.domain import DateFormat
from pipelines.utils.metadata.policy import (
    CoverageIds,
    assert_coverage_topology,
    compute_coverage_ranges,
    needs_row_access_policy,
)

FREE_ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"
PRO_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
IDS_BOTH = CoverageIds(free=FREE_ID, pro=PRO_ID)
IDS_FREE_ONLY = CoverageIds(free=FREE_ID, pro=None)

# The content date column each part_bdpro table paywalls on.
PART_BDPRO_DATE_COL = {
    "ceis": "data_inicio_sancao",
    "cnep": "data_inicio_sancao",
    "acordos_leniencia": "data_inicio_acordo",
}
ALL_FREE_TABLES = ["cepim", "acordos_leniencia_efeitos"]


def _spec(tier: Tier, column: str) -> CoverageSpec:
    return CoverageSpec(
        tier=tier,
        date_column_kind="date",
        coverage_columns=[CoverageColumn(name=column, entity="date")],
    )


PART_BDPRO_SPECS = {
    t: _spec("part_bdpro", col) for t, col in PART_BDPRO_DATE_COL.items()
}
ALL_FREE_SPECS = {
    t: _spec("all_free", "data_extracao") for t in ALL_FREE_TABLES
}


def test_date_format_is_year_md():
    """Every spec here is a full date (not year/year_month) — `DateFormat.YEAR_MD`."""
    for spec in [*PART_BDPRO_SPECS.values(), *ALL_FREE_SPECS.values()]:
        assert date_format_for(spec.date_column_kind) == DateFormat.YEAR_MD


def test_needs_row_access_policy_only_part_bdpro():
    for spec in PART_BDPRO_SPECS.values():
        assert needs_row_access_policy(spec) is True
    for spec in ALL_FREE_SPECS.values():
        assert needs_row_access_policy(spec) is False


def test_topology_part_bdpro_requires_free_and_pro():
    spec = PART_BDPRO_SPECS["ceis"]
    # both coverages present -> ok
    assert_coverage_topology(spec, IDS_BOTH)
    # onboarded AllFree (pro missing) -> hard fail before any write
    with pytest.raises(ValueError, match="part_bdpro"):
        assert_coverage_topology(spec, IDS_FREE_ONLY)


def test_topology_all_free_forbids_pro():
    spec = ALL_FREE_SPECS["cepim"]
    assert_coverage_topology(spec, IDS_FREE_ONLY)
    with pytest.raises(ValueError, match="all_free"):
        assert_coverage_topology(spec, IDS_BOTH)


def test_window_split_at_real_source_end():
    """free ends 6 months before the sanction-start max; pro spans the day after
    free_end through source_end, on the content date column."""
    spec = PART_BDPRO_SPECS["ceis"]
    source_end = date(2026, 8, 13)
    ranges = compute_coverage_ranges(spec, source_end, IDS_BOTH)
    free, pro = ranges.free, ranges.pro
    assert free is not None and pro is not None

    # free ends inclusive at source_end - 6 months
    assert ranges.free_end == date(2026, 2, 13)
    assert (free.endYear, free.endMonth, free.endDay) == (2026, 2, 13)

    # pro starts the next day (mutually exclusive) and ends at source_end
    assert (pro.startYear, pro.startMonth, pro.startDay) == (2026, 2, 14)
    assert (pro.endYear, pro.endMonth, pro.endDay) == (2026, 8, 13)


def test_window_rolls_when_source_advances():
    """As the sanction-start max advances, free_end and the pro window advance
    with it — the window is recomputed every run, nothing static."""
    spec = PART_BDPRO_SPECS["ceis"]
    r1 = compute_coverage_ranges(spec, date(2026, 8, 13), IDS_BOTH)
    r2 = compute_coverage_ranges(spec, date(2026, 9, 30), IDS_BOTH)
    pro2 = r2.pro
    assert r1.free_end == date(2026, 2, 13)
    assert r2.free_end == date(2026, 3, 30)  # rolled forward
    assert pro2 is not None
    assert (pro2.endYear, pro2.endMonth, pro2.endDay) == (2026, 9, 30)
    assert (pro2.startYear, pro2.startMonth, pro2.startDay) == (2026, 3, 31)


def test_all_free_range_ends_at_source_end():
    spec = ALL_FREE_SPECS["cepim"]
    source_end = date(2026, 8, 12)
    ranges = compute_coverage_ranges(spec, source_end, IDS_FREE_ONLY)
    free = ranges.free
    assert ranges.pro is None
    assert ranges.free_end == source_end
    assert free is not None
    assert (free.endYear, free.endMonth, free.endDay) == (2026, 8, 12)
