"""
Tipo 5 — testes puros da camada Domain (`pipelines/utils/metadata/domain.py`).

Cada teste mapeia a uma regra do catálogo §3.1 de `refatorar_backend.md`.
Sem rede, sem backend: validação acontece na construção do modelo Pydantic.
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from pipelines.utils.metadata.domain import (
    AllBdpro,
    AllFree,
    CoverageSpec,
    DateFormat,
    DateOnly,
    FreeLag,
    NonHistorical,
    PartBdpro,
    YearMonth,
    YearOnly,
    YearQuarter,
)


# --- R3: DateFormat -----------------------------------------------------------
def test_dateformat_values():
    assert DateFormat.YEAR.value == "%Y"
    assert DateFormat.YEAR_MONTH.value == "%Y-%m"
    assert DateFormat.YEAR_MD.value == "%Y-%m-%d"


# --- R2: DateColumn shapes ----------------------------------------------------
def test_date_column_variants_construct():
    assert DateOnly(col="data").kind == "date"
    assert YearOnly(col="ano").kind == "year"
    assert YearMonth(year="ano", month="mes").kind == "year_month"
    assert YearQuarter(year="ano", quarter="trimestre").kind == "year_quarter"


def test_year_month_requires_both_fields():
    with pytest.raises(ValidationError):
        # pyrefly: ignore [missing-argument]
        YearMonth(year="ano")  # falta month


# --- R1 + R4: CoverageSpec e validação cruzada coluna x formato -----------------
@pytest.mark.parametrize(
    "column,fmt,ok",
    [
        (DateOnly(col="data"), DateFormat.YEAR_MD, True),
        (YearOnly(col="ano"), DateFormat.YEAR, True),
        (YearMonth(year="ano", month="mes"), DateFormat.YEAR_MONTH, True),
        (YearQuarter(year="ano", quarter="tri"), DateFormat.YEAR_MONTH, True),
        # incompatíveis — hoje passam silenciosamente; aqui são recusados (R4):
        (YearOnly(col="ano"), DateFormat.YEAR_MD, False),
        (DateOnly(col="data"), DateFormat.YEAR, False),
        (YearMonth(year="ano", month="mes"), DateFormat.YEAR_MD, False),
    ],
)
def test_coverage_column_format_crosscheck(column, fmt, ok):
    if ok:
        spec = AllFree(date_column=column, date_format=fmt)
        assert spec.tier == "all_free"
    else:
        with pytest.raises(ValidationError):
            AllFree(date_column=column, date_format=fmt)


def test_all_bdpro_same_crosscheck():
    with pytest.raises(ValidationError):
        AllBdpro(
            date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR_MD
        )


# --- R5: FreeLag --------------------------------------------------------------
def test_free_lag_default_on_part_bdpro():
    spec = PartBdpro(
        date_column=DateOnly(col="data"), date_format=DateFormat.YEAR_MD
    )
    assert spec.free_lag.unit == "months"
    assert spec.free_lag.value == 6


def test_free_lag_value_must_be_positive():
    with pytest.raises(ValidationError):
        # pyrefly: ignore [bad-argument-type]
        FreeLag(unit="months", value=0)


def test_free_lag_as_relativedelta():
    from dateutil.relativedelta import relativedelta

    assert FreeLag(unit="months", value=6).as_relativedelta() == relativedelta(
        months=6
    )
    assert FreeLag(unit="years", value=2).as_relativedelta() == relativedelta(
        years=2
    )


# --- NonHistorical: frozen, sem date_column -----------------------------------
def test_non_historical_is_frozen():
    spec = NonHistorical()
    assert spec.tier == "non_historical"
    with pytest.raises(ValidationError):
        # pyrefly: ignore [read-only]
        spec.tier = "all_free"  # frozen → imutável


def test_non_historical_has_no_date_fields():
    spec = NonHistorical()
    assert not hasattr(spec, "date_column")
    assert not hasattr(spec, "date_format")


# --- CoverageSpec como union discriminada (parsing a partir de dict) ----------
def test_coverage_spec_dispatches_by_tier():
    adapter = TypeAdapter(CoverageSpec)
    spec = adapter.validate_python(
        {
            "tier": "part_bdpro",
            "date_column": {"kind": "date", "col": "data"},
            "date_format": "%Y-%m-%d",
        }
    )
    assert isinstance(spec, PartBdpro)
    # pyrefly: ignore [missing-attribute]
    assert spec.date_column.col == "data"


def test_coverage_spec_rejects_unknown_tier():
    adapter = TypeAdapter(CoverageSpec)
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {"tier": "premium", "date_column": {}, "date_format": "%Y"}
        )
