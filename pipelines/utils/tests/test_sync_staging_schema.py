"""Testes de `_sync_staging_schema`, que acrescenta colunas novas à staging.

Regressão para us_treasury_usaspending: a staging guardava
`outlayed_amount_from_IIJA_...` e o arquivo novo trazia `..._iija_...`. O
BigQuery trata as duas grafias como a mesma coluna, mas a comparação
case-sensitive tentava acrescentá-la e o PATCH falhava com
"already exists in schema".
"""

from __future__ import annotations

from typing import Any

import pytest
from google.cloud import bigquery

from pipelines.utils import tasks as t


class _FakeTable:
    def __init__(self, names: list[str]) -> None:
        self.schema = [bigquery.SchemaField(n, "STRING") for n in names]


class _FakeClient:
    def __init__(self, table: _FakeTable) -> None:
        self.table = table
        self.updates = 0

    def get_table(self, _ref: Any) -> _FakeTable:
        return self.table

    def update_table(self, table: _FakeTable, _fields: list[str]) -> None:
        self.updates += 1


class _FakeTb:
    table_full_name = {"staging": "p.ds_staging.t"}

    def __init__(self, current: list[str], incoming: list[str]) -> None:
        self.client = {"bigquery_staging": _FakeClient(_FakeTable(current))}
        self._incoming = [bigquery.SchemaField(n, "STRING") for n in incoming]

    def _load_staging_schema_from_data(self, **_kwargs: Any) -> list:
        return self._incoming


@pytest.fixture(autouse=True)
def _no_header(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(t, "dump_header", lambda **_kwargs: "header")


def test_case_only_difference_is_not_a_new_column() -> None:
    tb = _FakeTb(
        current=["year", "amount_from_IIJA"],
        incoming=["year", "amount_from_iija"],
    )
    # pyrefly: ignore [bad-argument-type]
    t._sync_staging_schema(tb=tb, data_path="x", source_format="parquet")
    assert tb.client["bigquery_staging"].updates == 0


def test_new_column_is_added() -> None:
    tb = _FakeTb(current=["year"], incoming=["year", "new_col"])
    # pyrefly: ignore [bad-argument-type]
    t._sync_staging_schema(tb=tb, data_path="x", source_format="parquet")
    client = tb.client["bigquery_staging"]
    assert client.updates == 1
    assert [f.name for f in client.table.schema] == ["year", "new_col"]
