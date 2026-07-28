"""
Testes do processo de poll (modelo novo) — `pipelines/utils/metadata/poll.py`.

Cobre as 3 funções puras com `FakeMetadataClient`/`FakeBQ` (conftest): quando cada
registro é escrito, o negativo (o que NÃO foi tocado), e a mudança-chave — o
`Table.Update` grava a cobertura (`source_end`), não o `last_modified`.
"""

import datetime

import pytest
from conftest import FakeBQ, FakeMetadataClient

from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    NonHistorical,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.policy import CoverageIds
from pipelines.utils.metadata.poll import (
    check_source_is_ahead_of_table,
    register_source_coverage,
    sync_table_coverage,
)


def _table_update_latest(client):
    """Devolve o `latest` gravado no Table.Update pelo fake."""
    for entity, _args, kwargs in client.writes:
        if entity == "table_update":
            return kwargs["latest"]
    raise AssertionError("nenhum table_update foi escrito")


# =============================================== register_source_coverage
def test_register_source_coverage_advances_when_source_is_newer():
    client = FakeMetadataClient(
        raw_source_update_latest=datetime.date(2026, 5, 1)
    )
    result = register_source_coverage(
        client, "br_x", "tab", source_max_date=datetime.date(2026, 6, 1)
    )
    assert result is True
    assert client.written_entities == ["poll", "raw_source_update"]


def test_register_source_coverage_first_time_advances():
    # RawDataSource.Update ainda não existe (None) -> qualquer data é nova.
    client = FakeMetadataClient(raw_source_update_latest=None)
    result = register_source_coverage(
        client, "br_x", "tab", source_max_date=datetime.date(2026, 6, 1)
    )
    assert result is True
    assert client.written_entities == ["poll", "raw_source_update"]


def test_register_source_coverage_none_writes_only_poll():
    client = FakeMetadataClient()
    result = register_source_coverage(
        client, "br_x", "tab", source_max_date=None
    )
    assert result is False
    assert client.written_entities == ["poll"]


def test_register_source_coverage_stale_writes_only_poll():
    client = FakeMetadataClient(
        raw_source_update_latest=datetime.date(2026, 6, 1)
    )
    result = register_source_coverage(
        client, "br_x", "tab", source_max_date=datetime.date(2026, 1, 1)
    )
    assert result is False
    assert client.written_entities == ["poll"]


# =========================================== check_source_is_ahead_of_table
def test_check_true_when_source_ahead():
    client = FakeMetadataClient(
        raw_source_update_latest=datetime.date(2026, 6, 1),
        table_update_latest=datetime.date(2026, 5, 1),
    )
    assert check_source_is_ahead_of_table(client, "br_x", "tab") is True


def test_check_false_when_caught_up():
    client = FakeMetadataClient(
        raw_source_update_latest=datetime.date(2026, 5, 1),
        table_update_latest=datetime.date(2026, 5, 1),
    )
    assert check_source_is_ahead_of_table(client, "br_x", "tab") is False


def test_check_false_when_source_has_no_update():
    client = FakeMetadataClient(
        raw_source_update_latest=None,
        table_update_latest=datetime.date(2026, 5, 1),
    )
    assert check_source_is_ahead_of_table(client, "br_x", "tab") is False


def test_check_true_when_table_never_materialized():
    client = FakeMetadataClient(
        raw_source_update_latest=datetime.date(2026, 5, 1),
        table_update_latest=None,
    )
    assert check_source_is_ahead_of_table(client, "br_x", "tab") is True


# =============================================== sync_table_coverage
def _part_bdpro():
    return PartBdpro(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    )


def _both_coverages():
    return CoverageIds(
        free="ffffffff-ffff-4fff-8fff-ffffffffffff",
        pro="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    )


def test_sync_part_bdpro_writes_coverages_table_update_and_rap():
    client = FakeMetadataClient(coverage_ids=_both_coverages())
    bq = FakeBQ(
        max_date=datetime.date(2026, 6, 1),
        last_modified=datetime.datetime(2026, 7, 15),
        can_read=True,
    )
    sync_table_coverage(client, bq, "br_x", "tab", _part_bdpro())

    assert client.written_entities == ["coverage", "coverage", "table_update"]
    assert len(bq.rap_calls) == 1


def test_sync_table_update_grava_cobertura_nao_last_modified():
    # Mudança-chave do modelo: Table.Update = source_end (max_date), e NÃO o
    # last_modified (horário de execução). Datas diferentes de propósito.
    client = FakeMetadataClient(coverage_ids=_both_coverages())
    bq = FakeBQ(
        max_date=datetime.date(2026, 6, 1),
        last_modified=datetime.datetime(2026, 7, 15),
        can_read=True,
    )
    sync_table_coverage(client, bq, "br_x", "tab", _part_bdpro())
    assert _table_update_latest(client) == datetime.date(2026, 6, 1)


def test_sync_all_free_no_rap():
    client = FakeMetadataClient(
        coverage_ids=CoverageIds(
            free="ffffffff-ffff-4fff-8fff-ffffffffffff", pro=None
        )
    )
    bq = FakeBQ(
        max_date=datetime.date(2026, 6, 1),
        last_modified=datetime.datetime(2026, 7, 15),
        can_read=True,
    )
    spec = AllFree(
        date_column=DateOnly(col="data"), date_format=DateFormat.YEAR_MD
    )
    sync_table_coverage(client, bq, "br_x", "tab", spec)

    assert client.written_entities == ["coverage", "table_update"]
    assert bq.rap_calls == []


def test_sync_non_historical_stamps_last_modified():
    client = FakeMetadataClient()
    bq = FakeBQ(last_modified=datetime.datetime(2026, 7, 15), can_read=True)
    sync_table_coverage(client, bq, "br_x", "tab", NonHistorical())

    assert client.written_entities == ["table_update"]
    assert _table_update_latest(client) == datetime.datetime(2026, 7, 15)


def test_sync_skips_table_update_when_cannot_read():
    client = FakeMetadataClient(coverage_ids=_both_coverages())
    bq = FakeBQ(max_date=datetime.date(2026, 6, 1), can_read=False)
    sync_table_coverage(client, bq, "br_x", "tab", _part_bdpro())

    assert "table_update" not in client.written_entities


def test_sync_blocked_in_prod_with_nonprod_data():
    client = FakeMetadataClient(table_status="published")
    bq = FakeBQ(max_date=datetime.date(2026, 6, 1))
    with pytest.raises(ValueError, match="under_review"):
        sync_table_coverage(
            client,
            bq,
            "br_x",
            "tab",
            _part_bdpro(),
            env="prod",
            bq_project="basedosdados-dev",
        )
    assert client.written_entities == []
