"""Tests for `extract_last_date_from_bq` — query construction.

What matters here is the generated SQL: `test_bq.py` mocks the whole function,
so without these tests the future-date filter is covered by nothing.
"""

import datetime
from unittest.mock import patch

import pandas as pd

from pipelines.utils.metadata.utils import extract_last_date_from_bq


def _query_for(date_column: dict, date_format: str = "%Y-%m-%d") -> str:
    """Run the function with BigQuery mocked and return the SQL it built."""
    captured = {}

    def fake_read_sql(query, billing_project_id, from_file):
        captured["query"] = query
        return pd.DataFrame({"max_date": [datetime.date(2026, 7, 31)]})

    with patch(
        "pipelines.utils.metadata.utils.bd.read_sql", side_effect=fake_read_sql
    ):
        extract_last_date_from_bq(
            dataset_id="ds",
            table_id="tb",
            date_format=date_format,
            date_column=date_column,
            billing_project_id="proj",
        )
    return captured["query"]


def test_date_column_ignores_future_dates():
    """Date column: future dates are filer typos and stay out.

    Without this, a single row with an impossible date pushes `free_end`
    forward and releases for free the window that should be BD Pro.
    """
    query = _query_for({"date": "transaction_date"})
    assert "WHERE transaction_date <= CURRENT_DATE()" in query
    assert "MAX(transaction_date)" in query


def test_year_column_keeps_future_labels():
    """Year labels a period: a future year is often legitimate (budget year,
    crop year), so it is not filtered."""
    query = _query_for({"year": "ano"}, date_format="%Y")
    assert "CURRENT_DATE()" not in query
    assert "MAX(DATE(ano,1,1))" in query


def test_year_month_column_keeps_future_labels():
    query = _query_for({"year": "ano", "month": "mes"}, date_format="%Y-%m")
    assert "CURRENT_DATE()" not in query
    assert "MAX(DATE(ano,mes,1))" in query


def test_year_quarter_column_keeps_future_labels():
    query = _query_for(
        {"year": "ano", "quarter": "trimestre"}, date_format="%Y-%m"
    )
    assert "CURRENT_DATE()" not in query
    assert "MAX(DATE(ano,trimestre*3,1))" in query


def test_returned_date_is_parsed_with_the_given_format():
    """The filter must not change the value that comes back."""
    captured = {}

    def fake_read_sql(query, billing_project_id, from_file):
        captured["query"] = query
        return pd.DataFrame({"max_date": [datetime.date(2026, 7, 31)]})

    with patch(
        "pipelines.utils.metadata.utils.bd.read_sql", side_effect=fake_read_sql
    ):
        out = extract_last_date_from_bq(
            dataset_id="ds",
            table_id="tb",
            date_format="%Y-%m-%d",
            date_column={"date": "transaction_date"},
            billing_project_id="proj",
        )
    assert out == "2026-07-31"
