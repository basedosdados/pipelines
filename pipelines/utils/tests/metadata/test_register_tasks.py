"""
Testes dos wrappers Prefect @task (`tasks.py`, camada de adaptação dos flows).

Confirmam que os wrappers constroem client/bq a partir de env/bq_project e
delegam para as funções puras. Invocados via `.fn` (Prefect 3) com
`MetadataClient`/`BigQueryReader` mockados.
"""

import datetime
from unittest.mock import patch

import pytest
from conftest import FakeBQ, FakeMetadataClient

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth
from pipelines.utils.metadata.policy import CoverageIds
from pipelines.utils.metadata.tasks import (
    _coerce_to_date,
    register_source_poll_task,
    register_table_materialization_task,
)


class TestCoerceToDate:
    """Regressão do bug pego em prod: o flow passa `source_max_date` como
    string (`get_latest_file` devolve `"2026-04"`) e a policy comparava `str`
    com `date` → TypeError. A coerção no wrapper resolve."""

    def test_string_year_month(self):
        assert _coerce_to_date("2026-04", "%Y-%m") == datetime.date(2026, 4, 1)

    def test_string_year_month_day(self):
        assert _coerce_to_date("2026-04-15", "%Y-%m-%d") == datetime.date(
            2026, 4, 15
        )

    def test_date_passthrough(self):
        d = datetime.date(2026, 4, 1)
        assert _coerce_to_date(d, "%Y-%m") is d

    def test_datetime_to_date(self):
        assert _coerce_to_date(
            datetime.datetime(2026, 4, 1, 13, 30), "%Y-%m"
        ) == datetime.date(2026, 4, 1)

    def test_none(self):
        assert _coerce_to_date(None, "%Y-%m") is None

    def test_unexpected_type_raises(self):
        with pytest.raises(TypeError):
            _coerce_to_date(123, "%Y-%m")


def test_source_poll_task_coerces_string_source_date():
    """O caminho exato do flow br_bcb_agencia: string '%Y-%m' + delega."""
    fake = FakeMetadataClient(
        raw_source_update_latest=datetime.date(2026, 1, 1)
    )
    with patch(
        "pipelines.utils.metadata.tasks.MetadataClient", return_value=fake
    ):
        result = register_source_poll_task.fn(
            "br_bcb_agencia",
            "agencia",
            source_max_date="2026-04",
            env="prod",
            date_format="%Y-%m",
        )
    assert result is True
    assert fake.written_entities == ["poll", "raw_source_update"]


def test_source_poll_task_builds_client_with_env_and_delegates():
    fake = FakeMetadataClient(
        raw_source_update_latest=datetime.date(2026, 1, 1)
    )
    with patch(
        "pipelines.utils.metadata.tasks.MetadataClient", return_value=fake
    ) as mk:
        result = register_source_poll_task.fn(
            "br_x",
            "tab",
            source_max_date=datetime.date(2026, 6, 1),
            env="prod",
        )
    mk.assert_called_once_with(env="prod")
    assert result is True
    assert fake.written_entities == ["poll", "raw_source_update"]


def test_materialization_task_builds_client_and_bq_and_delegates():
    fake = FakeMetadataClient(
        coverage_ids=CoverageIds(
            free="ffffffff-ffff-4fff-8fff-ffffffffffff",
            pro="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        )
    )
    fake_bq = FakeBQ(
        max_date=datetime.date(2026, 6, 1),
        last_modified=datetime.datetime(2026, 6, 2),
        can_read=True,
    )
    coverage = PartBdpro(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    )
    with (
        patch(
            "pipelines.utils.metadata.tasks.MetadataClient",
            return_value=fake,
        ),
        patch(
            "pipelines.utils.metadata.tasks.BigQueryReader",
            return_value=fake_bq,
        ),
    ):
        register_table_materialization_task.fn(
            "br_x",
            "tab",
            coverage,
            env="prod",
            bq_project="basedosdados",
            prefect_mode="prod",
        )
    assert fake.written_entities == ["coverage", "coverage", "table_update"]
    assert len(fake_bq.rap_calls) == 1
