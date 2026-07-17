"""
Tipo 5 — testes puros da camada DTO (`pipelines/utils/metadata/dto.py`).

Validam o que sai pelo fio (R18, ranges numéricos, UUIDs, ISO 8601). Provam que
os bugs latentes da §1.5 deixam de ser representáveis.
"""

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from pipelines.utils.metadata.dto import (
    DateTimeRangeInput,
    PollInput,
    RawSourceUpdateInput,
    TableUpdateInput,
    _compose_end_date,
)

UUID = "00000000-0000-4000-8000-000000000000"


# ------------------------------------------------- _compose_end_date
def test_compose_end_date_matches_read_max_date_convention():
    """Mês/dia ausentes viram 1, igual ao `strptime` de `bq.read_max_date`.

    Uma cobertura anual que termina em 2025 tem de virar 2025-01-01 — o mesmo
    que `strptime("2025", "%Y")` devolve. Completar com o fim do período
    (2025-12-31) faria toda tabela anual parecer defasada perante a própria
    fonte e disparar ingestão a cada run.
    """
    assert _compose_end_date({"endYear": 2025}) == date(2025, 1, 1)
    assert _compose_end_date(
        {"endYear": 2025, "endMonth": None, "endDay": None}
    ) == date(2025, 1, 1)
    assert (
        _compose_end_date({"endYear": 2025})
        == datetime.strptime("2025", "%Y").date()
    )


def test_compose_end_date_keeps_month_and_day():
    assert _compose_end_date({"endYear": 2026, "endMonth": 6}) == date(
        2026, 6, 1
    )
    assert _compose_end_date(
        {"endYear": 2026, "endMonth": 6, "endDay": 15}
    ) == date(2026, 6, 15)


def test_compose_end_date_without_year_is_none():
    assert _compose_end_date({}) is None
    assert _compose_end_date({"endYear": None, "endMonth": 6}) is None


BAD_UUID = "not-a-uuid"


# --- DateTimeRangeInput: R18 shape + ranges -----------------------------------
def test_datetimerange_full_valid():
    dto = DateTimeRangeInput(coverage=UUID, endYear=2026, endMonth=6, endDay=1)
    assert dto.endYear == 2026


def test_datetimerange_year_only_valid():
    assert DateTimeRangeInput(coverage=UUID, endYear=2026).endMonth is None


def test_datetimerange_day_without_month_rejected():  # R18
    with pytest.raises(ValidationError, match="endDay"):
        DateTimeRangeInput(coverage=UUID, endYear=2026, endDay=5)


def test_datetimerange_month_without_year_rejected():  # R18
    with pytest.raises(ValidationError, match="startMonth"):
        DateTimeRangeInput(coverage=UUID, startMonth=6)


@pytest.mark.parametrize("bad", [1899, 2101])
def test_datetimerange_year_out_of_range(bad):
    with pytest.raises(ValidationError):
        DateTimeRangeInput(coverage=UUID, endYear=bad)


@pytest.mark.parametrize("bad", [0, 13])
def test_datetimerange_month_out_of_range(bad):
    with pytest.raises(ValidationError):
        DateTimeRangeInput(coverage=UUID, endYear=2026, endMonth=bad)


def test_datetimerange_bad_coverage_uuid():
    with pytest.raises(ValidationError):
        DateTimeRangeInput(coverage=BAD_UUID, endYear=2026)


# --- PollInput: FK, ISO 8601, defaults ----------------------------------------
def test_poll_input_valid():
    dto = PollInput(rawDataSource=UUID, latest="2026-06-01", entity=UUID)
    assert dto.frequency == 1  # default


def test_poll_input_coerces_date_to_iso():
    # backend usa scalar DateTime: date-only vira datetime à meia-noite
    dto = PollInput(rawDataSource=UUID, latest=date(2026, 6, 1), entity=UUID)
    assert dto.latest == "2026-06-01T00:00:00"


def test_poll_input_coerces_datetime_to_iso():
    dto = PollInput(
        rawDataSource=UUID, latest=datetime(2026, 6, 1, 12, 30), entity=UUID
    )
    assert dto.latest.startswith("2026-06-01T12:30")


def test_poll_input_rejects_non_iso_latest():  # mata o caminho do bug "Y%"/sentinel
    with pytest.raises(ValidationError):
        PollInput(rawDataSource=UUID, latest="not-a-date", entity=UUID)


def test_poll_input_requires_entity():
    with pytest.raises(ValidationError):
        PollInput(rawDataSource=UUID, latest="2026-06-01")  # entity ausente


def test_poll_input_bad_raw_source_uuid():
    with pytest.raises(ValidationError):
        PollInput(rawDataSource=BAD_UUID, latest="2026-06-01", entity=UUID)


# --- TableUpdateInput vs RawSourceUpdateInput: FK distinto --------------------
def test_table_update_uses_table_fk():
    dto = TableUpdateInput(table=UUID, latest="2026-06-01")
    assert dto.table == UUID
    assert not hasattr(dto, "rawDataSource")


def test_raw_source_update_uses_raw_source_fk():
    dto = RawSourceUpdateInput(rawDataSource=UUID, latest="2026-06-01")
    assert dto.rawDataSource == UUID
    assert not hasattr(dto, "table")


def test_update_inputs_optional_entity_and_frequency():
    assert TableUpdateInput(table=UUID, latest="2026-06-01").entity is None
    assert (
        RawSourceUpdateInput(rawDataSource=UUID, latest="2026-06-01").frequency
        is None
    )
