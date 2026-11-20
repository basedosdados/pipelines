"""Testes de `reload_window`, sem rede nem backend."""

from datetime import date

from pipelines.datasets.br_ms_sih.tasks import reload_window
from pipelines.shared.datasus.tasks import parse_year_months


def test_window_ends_at_reference_month() -> None:
    """A janela termina na competência nova e volta as anteriores."""
    assert reload_window(date(2026, 9, 1), 3) == "2607 2608 2609"


def test_window_crosses_year_boundary() -> None:
    """Em janeiro, a janela volta para novembro e dezembro do ano anterior."""
    assert reload_window(date(2027, 1, 1), 3) == "2611 2612 2701"


def test_window_is_readable_by_parse_year_months() -> None:
    """O texto da janela passa pelo mesmo parser do backfill."""
    assert parse_year_months(reload_window(date(2026, 9, 1), 3)) == {
        "2607",
        "2608",
        "2609",
    }
