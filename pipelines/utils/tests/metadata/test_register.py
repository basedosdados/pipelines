"""
Tipo 4 — testes dos orquestradores (`pipelines/utils/metadata/register.py`).

A matriz de escrita da §1.8 vira asserção: para cada cenário, quais entidades
foram escritas, em que ordem — e, crucialmente, quais NÃO foram (o negativo).

Usa `FakeMetadataClient`/`FakeBQ` (conftest). Sem rede, sem BQ.
"""

import datetime

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
from pipelines.utils.metadata.register import (
    register_source_poll,
    register_source_poll_by_size,
    register_table_materialization,
)


class FakeRedis:
    """Double in-memory de RedisPal (get/set de um dict por chave)."""

    def __init__(self, store=None):
        self.store = store or {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value


# ===================================================== register_source_poll (§1.8)
def test_poll_without_news_writes_only_poll():
    client = FakeMetadataClient()
    result = register_source_poll(client, "br_x", "tab", source_max_date=None)
    assert result is False
    assert client.written_entities == [
        "poll"
    ]  # Poll sempre; Update nunca (R15/R16)


def test_poll_with_news_writes_poll_then_update():
    client = FakeMetadataClient(
        raw_source_update_latest=datetime.date(2026, 1, 1)
    )
    result = register_source_poll(
        client, "br_x", "tab", source_max_date=datetime.date(2026, 6, 1)
    )
    assert result is True
    assert client.written_entities == [
        "poll",
        "raw_source_update",
    ]  # ordem importa


def test_poll_with_stale_source_writes_only_poll():
    client = FakeMetadataClient(
        raw_source_update_latest=datetime.date(2026, 6, 1)
    )
    result = register_source_poll(
        client, "br_x", "tab", source_max_date=datetime.date(2026, 1, 1)
    )
    assert result is False
    assert client.written_entities == ["poll"]


def test_poll_latest_is_today():
    client = FakeMetadataClient()
    register_source_poll(client, "br_x", "tab")
    _, _, kwargs = client.writes[0]
    assert kwargs["latest"].date() == datetime.datetime.today().date()


# ============================================ register_table_materialization (§1.8)
def _part_bdpro():
    return PartBdpro(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    )


def test_part_bdpro_writes_coverages_table_update_and_rap():
    client = FakeMetadataClient(
        coverage_ids=CoverageIds(
            free="ffffffff-ffff-4fff-8fff-ffffffffffff",
            pro="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        )
    )
    bq = FakeBQ(
        max_date=datetime.date(2026, 6, 1),
        last_modified=datetime.datetime(2026, 6, 2),
        can_read=True,
    )
    register_table_materialization(client, bq, "br_x", "tab", _part_bdpro())

    # free + pro coverage, depois table_update (W**), RAP aplicada (W***)
    assert client.written_entities == ["coverage", "coverage", "table_update"]
    assert len(bq.rap_calls) == 1


def test_all_free_writes_only_free_coverage_and_table_update_no_rap():
    client = FakeMetadataClient(
        coverage_ids=CoverageIds(
            free="ffffffff-ffff-4fff-8fff-ffffffffffff", pro=None
        )
    )
    bq = FakeBQ(
        max_date=datetime.date(2026, 6, 1),
        last_modified=datetime.datetime(2026, 6, 2),
        can_read=True,
    )
    spec = AllFree(
        date_column=DateOnly(col="data"), date_format=DateFormat.YEAR_MD
    )
    register_table_materialization(client, bq, "br_x", "tab", spec)

    assert client.written_entities == ["coverage", "table_update"]
    assert bq.rap_calls == []  # all_free não aplica RAP (R13)


def test_table_update_skipped_when_cannot_read_metadata():
    client = FakeMetadataClient(
        coverage_ids=CoverageIds(
            free="ffffffff-ffff-4fff-8fff-ffffffffffff",
            pro="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        )
    )
    bq = FakeBQ(
        max_date=datetime.date(2026, 6, 1), can_read=False
    )  # R12 → skip
    register_table_materialization(client, bq, "br_x", "tab", _part_bdpro())

    assert "table_update" not in client.written_entities
    assert client.written_entities == ["coverage", "coverage"]


def test_non_historical_writes_only_table_update():
    client = FakeMetadataClient()
    bq = FakeBQ(last_modified=datetime.datetime(2026, 6, 2), can_read=True)
    register_table_materialization(client, bq, "br_x", "tab", NonHistorical())

    assert client.written_entities == ["table_update"]  # sem coverage, sem RAP


def test_write_blocked_in_prod_with_nonprod_data():
    import pytest

    client = FakeMetadataClient(table_status="published")
    bq = FakeBQ(max_date=datetime.date(2026, 6, 1))
    with pytest.raises(ValueError, match="under_review"):
        register_table_materialization(
            client,
            bq,
            "br_x",
            "tab",
            _part_bdpro(),
            env="prod",
            bq_project="basedosdados-dev",
        )
    assert (
        client.written_entities == []
    )  # R14 — nada escrito antes do fail-fast


# ================================ register_source_poll_by_size (poll por tamanho)
def test_poll_by_size_first_time_writes_poll_and_update():
    client = FakeMetadataClient()
    redis = FakeRedis()
    outdated = register_source_poll_by_size(client, redis, "br_x", "tab", 1000)
    assert outdated is True
    assert client.written_entities == ["poll", "raw_source_update"]
    assert redis.store["br_x"]["tab"]  # tamanho registrado


def test_poll_by_size_bigger_writes_poll_and_update():
    client = FakeMetadataClient()
    redis = FakeRedis({"br_x": {"tab": {"2020-01-01": 500}}})
    outdated = register_source_poll_by_size(client, redis, "br_x", "tab", 1000)
    assert outdated is True
    assert client.written_entities == ["poll", "raw_source_update"]


def test_poll_by_size_equal_writes_only_poll():
    client = FakeMetadataClient()
    redis = FakeRedis({"br_x": {"tab": {"2020-01-01": 1000}}})
    outdated = register_source_poll_by_size(client, redis, "br_x", "tab", 1000)
    assert outdated is False
    assert client.written_entities == ["poll"]  # sem Update


def test_poll_by_size_smaller_raises_after_poll():
    import pytest

    client = FakeMetadataClient()
    redis = FakeRedis({"br_x": {"tab": {"2020-01-01": 2000}}})
    with pytest.raises(ValueError, match="MENOR"):
        register_source_poll_by_size(client, redis, "br_x", "tab", 1000)
    assert client.written_entities == ["poll"]  # poll R15 antes do fail


def test_poll_by_size_keeps_last_10_records():
    client = FakeMetadataClient()
    history = {f"2020-01-{d:02d}": d for d in range(1, 11)}  # 10 registros
    redis = FakeRedis({"br_x": {"tab": dict(history)}})
    register_source_poll_by_size(client, redis, "br_x", "tab", 9999)
    assert len(redis.store["br_x"]["tab"]) <= 10
