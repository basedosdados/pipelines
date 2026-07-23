"""
Tipo 5 — testes puros da camada Policy (`pipelines/utils/metadata/policy.py`).

Regras R7-R16. Sem rede, sem BQ.
"""

from datetime import date

import pytest

from pipelines.utils.metadata.domain import (
    AllBdpro,
    AllFree,
    DateFormat,
    DateOnly,
    FreeLag,
    NonHistorical,
    PartBdpro,
    YearMonth,
    YearOnly,
)
from pipelines.utils.metadata.policy import (
    CoverageIds,
    assert_coverage_topology,
    assert_write_allowed,
    compute_coverage_ranges,
    needs_row_access_policy,
    should_update_raw_source,
)

FREE_ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"
PRO_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
IDS_BOTH = CoverageIds(free=FREE_ID, pro=PRO_ID)


# --------------------------------------------------------------------- R16
@pytest.mark.parametrize(
    "api,src,expected",
    [
        (date(2026, 1, 1), date(2026, 6, 1), True),  # fonte mais nova
        (date(2026, 6, 1), date(2026, 6, 1), False),  # iguais
        (date(2026, 6, 1), date(2026, 1, 1), False),  # fonte mais velha
        (None, date(2026, 6, 1), True),  # sem registro na api
        (date(2026, 1, 1), None, False),  # sem novidade
    ],
)
def test_should_update_raw_source(api, src, expected):
    assert should_update_raw_source(api, src) is expected


# --------------------------------------------------------------------- R14
def test_assert_write_allowed_blocks_prod_nonprod_not_under_review():
    with pytest.raises(ValueError, match="under_review"):
        assert_write_allowed("prod", "basedosdados-dev", "published")


def test_assert_write_allowed_ok_when_under_review():
    assert_write_allowed(
        "prod", "basedosdados-dev", "under_review"
    )  # não levanta


def test_assert_write_allowed_ok_in_dev():
    assert_write_allowed(
        "dev", "basedosdados-dev", "published"
    )  # dev não restringe


def test_assert_write_allowed_ok_prod_with_prod_project():
    assert_write_allowed("prod", "basedosdados", "published")


# --------------------------------------------------------------------- R13
def test_needs_row_access_policy_only_part_bdpro():
    part = PartBdpro(
        date_column=DateOnly(col="data"), date_format=DateFormat.YEAR_MD
    )
    free = AllFree(
        date_column=DateOnly(col="data"), date_format=DateFormat.YEAR_MD
    )
    assert needs_row_access_policy(part) is True
    assert needs_row_access_policy(free) is False
    assert needs_row_access_policy(NonHistorical()) is False


# ----------------------------------------------------------------- R7/R8/R9
def test_topology_part_bdpro_requires_both():
    spec = PartBdpro(
        date_column=DateOnly(col="data"), date_format=DateFormat.YEAR_MD
    )
    assert_coverage_topology(spec, CoverageIds(free="f", pro="p"))  # ok
    with pytest.raises(ValueError):
        assert_coverage_topology(spec, CoverageIds(free="f", pro=None))


def test_topology_all_free_requires_free_and_no_pro():
    spec = AllFree(
        date_column=DateOnly(col="data"), date_format=DateFormat.YEAR_MD
    )
    assert_coverage_topology(spec, CoverageIds(free="f", pro=None))  # ok
    with pytest.raises(ValueError):
        assert_coverage_topology(spec, CoverageIds(free="f", pro="p"))


def test_topology_all_bdpro_requires_pro_and_no_free():
    spec = AllBdpro(
        date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
    )
    assert_coverage_topology(spec, CoverageIds(free=None, pro="p"))  # ok
    with pytest.raises(ValueError):
        assert_coverage_topology(spec, CoverageIds(free="f", pro="p"))


# ------------------------------------------------------------------ R10/R11
def test_compute_all_free_daily():
    spec = AllFree(
        date_column=DateOnly(col="data"), date_format=DateFormat.YEAR_MD
    )
    r = compute_coverage_ranges(spec, date(2026, 6, 15), IDS_BOTH)
    assert r.pro is None
    # pyrefly: ignore [missing-attribute]
    assert r.free.coverage == FREE_ID
    # pyrefly: ignore [missing-attribute]
    assert (r.free.endYear, r.free.endMonth, r.free.endDay) == (2026, 6, 15)


def test_compute_part_bdpro_monthly_syncs_and_lags():
    spec = PartBdpro(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    )
    r = compute_coverage_ranges(spec, date(2026, 6, 1), IDS_BOTH)
    # pro termina em 2026-06
    # pyrefly: ignore [missing-attribute]
    assert (r.pro.endYear, r.pro.endMonth) == (2026, 6)
    # free termina 6 meses antes: 2025-12
    # pyrefly: ignore [missing-attribute]
    assert (r.free.endYear, r.free.endMonth) == (2025, 12)
    # pyrefly: ignore [missing-attribute]
    assert r.free.endDay is None  # granularidade mensal
    # R11: pro começa onde free termina
    # pyrefly: ignore [missing-attribute]
    assert (r.pro.startYear, r.pro.startMonth) == (2025, 12)
    assert r.free_end == date(2025, 12, 1)


def test_compute_all_bdpro_annual():
    spec = AllBdpro(
        date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
    )
    r = compute_coverage_ranges(
        spec, date(2026, 1, 1), CoverageIds(pro=PRO_ID)
    )
    assert r.free is None
    # pyrefly: ignore [missing-attribute]
    assert r.pro.endYear == 2026
    # pyrefly: ignore [missing-attribute]
    assert r.pro.endMonth is None


def test_compute_rejects_non_historical():
    with pytest.raises(ValueError):
        compute_coverage_ranges(NonHistorical(), date(2026, 1, 1), IDS_BOTH)
