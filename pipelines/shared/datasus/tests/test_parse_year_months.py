"""Leitura das competências pedidas (pipelines.shared.datasus)."""

import pytest

from pipelines.shared.datasus.tasks import parse_year_months


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2507", {"2507"}),
        ("202507", {"2507"}),
        ("2025-07", {"2507"}),
        ("2403 2404", {"2403", "2404"}),
        ("2024-03,2024-04, 2405", {"2403", "2404", "2405"}),
    ],
)
def test_parse_year_months(value, expected):
    assert parse_year_months(value) == expected


@pytest.mark.parametrize("value", ["2513", "abc", "24-3", "2403 99"])
def test_parse_year_months_rejects_bad_months(value):
    with pytest.raises(ValueError):
        parse_year_months(value)
