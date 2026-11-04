"""
Tipo 5 — testes puros da camada Policy (`pipelines/utils/metadata/policy.py`).

Regras R7-R16. Sem rede, sem BQ.
"""

from datetime import date

import pytest

from pipelines.utils.metadata.coverage import (
    CoverageColumn,
    CoverageSpec,
    DateColumnKind,
    Tier,
)
from pipelines.utils.metadata.policy import (
    PART_BDPRO_FREE_LAG,
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


def _spec(
    tier: Tier, date_column_kind: DateColumnKind = "", **columns: str
) -> CoverageSpec:
    """`columns` mapeia entity -> nome da coluna (ex.: `year="ano"`)."""
    return CoverageSpec(
        tier=tier,
        date_column_kind=date_column_kind,
        coverage_columns=[
            # pyrefly: ignore [bad-argument-type]
            CoverageColumn(name=name, entity=entity)
            for entity, name in columns.items()
        ],
    )


def test_part_bdpro_free_lag_is_six_months():
    """Generalizado pra todos os datasets (pipelines#2130) — não vem mais do
    backend nem varia por tabela."""
    assert PART_BDPRO_FREE_LAG.months == 6


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
    part = _spec("part_bdpro", "date", date="data")
    free = _spec("all_free", "date", date="data")
    non_historical = _spec("non_historical")
    assert needs_row_access_policy(part) is True
    assert needs_row_access_policy(free) is False
    assert needs_row_access_policy(non_historical) is False


# ----------------------------------------------------------------- R7/R8/R9
def test_topology_part_bdpro_requires_both():
    spec = _spec("part_bdpro", "date", date="data")
    assert_coverage_topology(spec, CoverageIds(free="f", pro="p"))  # ok
    with pytest.raises(ValueError):
        assert_coverage_topology(spec, CoverageIds(free="f", pro=None))


def test_topology_all_free_requires_free_and_no_pro():
    spec = _spec("all_free", "date", date="data")
    assert_coverage_topology(spec, CoverageIds(free="f", pro=None))  # ok
    with pytest.raises(ValueError):
        assert_coverage_topology(spec, CoverageIds(free="f", pro="p"))


def test_topology_all_bdpro_requires_pro_and_no_free():
    spec = _spec("all_bdpro", "year", year="ano")
    assert_coverage_topology(spec, CoverageIds(free=None, pro="p"))  # ok
    with pytest.raises(ValueError):
        assert_coverage_topology(spec, CoverageIds(free="f", pro="p"))


def test_topology_non_historical_never_raises():
    spec = _spec("non_historical")
    assert_coverage_topology(spec, CoverageIds(free=None, pro=None))
    assert_coverage_topology(spec, CoverageIds(free="f", pro="p"))


# ------------------------------------------------------------------ R10/R11
def test_compute_all_free_daily():
    spec = _spec("all_free", "date", date="data")
    r = compute_coverage_ranges(spec, date(2026, 6, 15), IDS_BOTH)
    assert r.pro is None
    # pyrefly: ignore [missing-attribute]
    assert r.free.coverage == FREE_ID
    # pyrefly: ignore [missing-attribute]
    assert (r.free.endYear, r.free.endMonth, r.free.endDay) == (2026, 6, 15)


def test_compute_part_bdpro_monthly_syncs_and_lags():
    spec = _spec("part_bdpro", "year_month", year="ano", month="mes")
    r = compute_coverage_ranges(spec, date(2026, 6, 1), IDS_BOTH)
    # pro termina em 2026-06
    # pyrefly: ignore [missing-attribute]
    assert (r.pro.endYear, r.pro.endMonth) == (2026, 6)
    # free termina PART_BDPRO_FREE_LAG (6 meses) antes: 2025-12
    # pyrefly: ignore [missing-attribute]
    assert (r.free.endYear, r.free.endMonth) == (2025, 12)
    # pyrefly: ignore [missing-attribute]
    assert r.free.endDay is None  # granularidade mensal
    # R11: pro starts in the period after free ends — `free_end` is inclusive
    # (the RAP grants `date <= free_end`), so free 2025-12 and pro 2026-01 do
    # not overlap. See `test_compute_part_bdpro_ranges_never_overlap`.
    # pyrefly: ignore [missing-attribute]
    assert (r.pro.startYear, r.pro.startMonth) == (2026, 1)
    assert r.free_end == date(2025, 12, 1)


def test_compute_part_bdpro_ranges_never_overlap():
    """free e pro não podem reivindicar o mesmo período, em nenhuma
    granularidade — inclusive nas viradas de ano/mês. `PART_BDPRO_FREE_LAG`
    é fixo (6 meses) pra todas, então o que varia por caso é só o
    `source_end` escolhido pra cair numa virada."""
    monthly = _spec("part_bdpro", "year_month", year="ano", month="mes")
    r = compute_coverage_ranges(monthly, date(2026, 6, 1), IDS_BOTH)
    # virada de ano: free termina 2025-12, pro começa 2026-01
    # pyrefly: ignore [missing-attribute]
    assert (r.free.endYear, r.free.endMonth) == (2025, 12)
    # pyrefly: ignore [missing-attribute]
    assert (r.pro.startYear, r.pro.startMonth) == (2026, 1)

    annual = _spec("part_bdpro", "year", year="ano")
    r = compute_coverage_ranges(annual, date(2026, 1, 1), IDS_BOTH)
    # anual avança um ano, não um mês
    # pyrefly: ignore [missing-attribute]
    assert r.free.endYear == 2025
    # pyrefly: ignore [missing-attribute]
    assert (r.pro.startYear, r.pro.startMonth) == (2026, None)

    daily = _spec("part_bdpro", "date", date="data")
    r = compute_coverage_ranges(daily, date(2026, 3, 31), IDS_BOTH)
    # virada de mês: free termina 2025-09-30 (set só tem 30 dias), pro
    # começa 2025-10-01
    # pyrefly: ignore [missing-attribute]
    assert (r.free.endYear, r.free.endMonth, r.free.endDay) == (2025, 9, 30)
    # pyrefly: ignore [missing-attribute]
    assert (r.pro.startYear, r.pro.startMonth, r.pro.startDay) == (
        2025,
        10,
        1,
    )


def test_compute_all_bdpro_annual():
    spec = _spec("all_bdpro", "year", year="ano")
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
        compute_coverage_ranges(
            _spec("non_historical"), date(2026, 1, 1), IDS_BOTH
        )
