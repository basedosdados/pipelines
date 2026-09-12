"""Pure-function tests for the br_cgu_sancoes BD Pro rolling window.

The paywall for ceis/cnep/acordos_leniencia is keyed on the *sanction start*
date (a content column, ``data_inicio_sancao`` / ``data_inicio_acordo``), not on
the snapshot/release date. These tests confirm the pure metadata policy accepts
that arbitrary DateOnly column and that the window rolls as the source advances.
No network, no BigQuery — ``apply_row_access_policies`` (the actual DDL) needs
the worker and is not exercised here.
"""

from datetime import date

import pytest

from pipelines.datasets.br_cgu_sancoes.flows import _COVERAGE
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    PartBdpro,
)
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

PART_BDPRO_TABLES = ["ceis", "cnep", "acordos_leniencia"]
ALL_FREE_TABLES = ["cepim", "acordos_leniencia_efeitos"]

# The content date column each part_bdpro table paywalls on.
PART_BDPRO_DATE_COL = {
    "ceis": "data_inicio_sancao",
    "cnep": "data_inicio_sancao",
    "acordos_leniencia": "data_inicio_acordo",
}


def _col(spec) -> str:
    dc = spec.date_column
    assert isinstance(dc, DateOnly)
    return dc.col


def test_flow_coverage_specs_are_wellformed():
    """The three compliance tables are part_bdpro on a content date, 6-month lag;
    the other two are all_free on the snapshot date."""
    for t in PART_BDPRO_TABLES:
        spec = _COVERAGE[t]
        assert isinstance(spec, PartBdpro)
        assert spec.date_format == DateFormat.YEAR_MD
        assert spec.free_lag.unit == "months" and spec.free_lag.value == 6
        assert _col(spec) == PART_BDPRO_DATE_COL[t]
    for t in ALL_FREE_TABLES:
        spec = _COVERAGE[t]
        assert isinstance(spec, AllFree)
        assert _col(spec) == "data_extracao"


def test_needs_row_access_policy_only_part_bdpro():
    for t in PART_BDPRO_TABLES:
        assert needs_row_access_policy(_COVERAGE[t]) is True
    for t in ALL_FREE_TABLES:
        assert needs_row_access_policy(_COVERAGE[t]) is False


def test_topology_part_bdpro_requires_free_and_pro():
    spec = _COVERAGE["ceis"]
    # both coverages present -> ok
    assert_coverage_topology(spec, IDS_BOTH)
    # onboarded AllFree (pro missing) -> hard fail before any write
    with pytest.raises(ValueError, match="part_bdpro"):
        assert_coverage_topology(spec, IDS_FREE_ONLY)


def test_topology_all_free_forbids_pro():
    spec = _COVERAGE["cepim"]
    assert_coverage_topology(spec, IDS_FREE_ONLY)
    with pytest.raises(ValueError, match="all_free"):
        assert_coverage_topology(spec, IDS_BOTH)


def test_window_split_at_real_source_end():
    """free ends 6 months before the sanction-start max; pro spans the day after
    free_end through source_end, on the content date column."""
    spec = _COVERAGE["ceis"]
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
    spec = _COVERAGE["ceis"]
    r1 = compute_coverage_ranges(spec, date(2026, 8, 13), IDS_BOTH)
    r2 = compute_coverage_ranges(spec, date(2026, 9, 30), IDS_BOTH)
    pro2 = r2.pro
    assert r1.free_end == date(2026, 2, 13)
    assert r2.free_end == date(2026, 3, 30)  # rolled forward
    assert pro2 is not None
    assert (pro2.endYear, pro2.endMonth, pro2.endDay) == (2026, 9, 30)
    assert (pro2.startYear, pro2.startMonth, pro2.startDay) == (2026, 3, 31)


def test_all_free_range_ends_at_source_end():
    spec = _COVERAGE["cepim"]
    source_end = date(2026, 8, 12)
    ranges = compute_coverage_ranges(spec, source_end, IDS_FREE_ONLY)
    free = ranges.free
    assert ranges.pro is None
    assert ranges.free_end == source_end
    assert free is not None
    assert (free.endYear, free.endMonth, free.endDay) == (2026, 8, 12)
