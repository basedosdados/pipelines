"""
Tipos 1-3 — testes da camada Transport (`pipelines/utils/metadata/client.py`).

- Tipo 1 (request-contract): para cada `upsert_*`, a mutation emitida tem o FK,
  o `latest` e a `entity` corretos; a `Entity` é resolvida (injetada), não um
  literal hardcoded.
- Tipo 2 (response-handling): R19 (≤1 nó), registro ausente → None, `errors[]`
  populado → exceção.
- Tipo 3 (auth): tokenAuth executado uma única vez por instância (cache).

Tudo offline: o `RecordingBackend` substitui o `bd.Backend`.
"""

import datetime

import pytest

# pyrefly: ignore [missing-import]
from conftest import RecordingBackend, mutation_response, node_response

from pipelines.utils.metadata.client import (
    BackendMutationError,
    MetadataClient,
)
from pipelines.utils.metadata.dto import DateTimeRangeInput

UUID = "00000000-0000-4000-8000-000000000000"
DAY_ENTITY = "00000000-0000-4000-8000-0000000000d1"
MONTH_ENTITY = "00000000-0000-4000-8000-0000000000e2"
RDS = "11111111-1111-4111-8111-111111111111"
TBL_PK = "22222222-2222-4222-8222-222222222222"  # == RecordingBackend.table_pk default


def _input(call: dict) -> dict:
    return call["variables"]["input"]


# ============================================================ TIPO 1 — request
def test_upsert_poll_create_emits_full_payload(client, backend):
    backend.set_response(
        "allRawdatasource", node_response("allRawdatasource", RDS)
    )
    backend.set_response(
        "allPoll", node_response("allPoll", None)
    )  # ausente → create
    backend.set_response(
        "CreateUpdatePoll", mutation_response("CreateUpdatePoll")
    )

    client.upsert_raw_source_poll(
        "br_x", "tab", latest=datetime.datetime(2026, 6, 1)
    )

    v = _input(backend.mutation_for("CreateUpdatePoll"))
    assert v["rawDataSource"] == RDS
    assert v["latest"] == "2026-06-01T00:00:00"
    assert v["frequency"] == 1
    assert v["entity"] == DAY_ENTITY  # resolvida, não 81f0c890… hardcoded
    assert "id" not in v  # create não envia id


def test_upsert_poll_update_sends_only_id_and_latest(client, backend):
    backend.set_response(
        "allRawdatasource", node_response("allRawdatasource", RDS)
    )
    backend.set_response(
        "allPoll", node_response("allPoll", "poll-9")
    )  # existe → update
    backend.set_response(
        "CreateUpdatePoll", mutation_response("CreateUpdatePoll")
    )

    client.upsert_raw_source_poll(
        "br_x", "tab", latest=datetime.date(2026, 6, 1)
    )

    v = _input(backend.mutation_for("CreateUpdatePoll"))
    assert v == {
        "id": "poll-9",
        "latest": "2026-06-01T00:00:00",  # date-only normalizado p/ DateTime
    }  # não reescreve entity/frequency


def test_upsert_table_update_uses_table_fk_not_rawdatasource(client, backend):
    backend.set_response("allUpdate", node_response("allUpdate", None))
    backend.set_response(
        "CreateUpdateUpdate", mutation_response("CreateUpdateUpdate")
    )

    client.upsert_table_update("br_x", "tab", latest=datetime.date(2026, 6, 1))

    v = _input(backend.mutation_for("CreateUpdateUpdate"))
    assert v["table"] == TBL_PK
    assert "rawDataSource" not in v
    assert v["entity"] == MONTH_ENTITY


def test_upsert_raw_source_update_uses_rawdatasource_fk(client, backend):
    backend.set_response(
        "allRawdatasource", node_response("allRawdatasource", RDS)
    )
    backend.set_response("allUpdate", node_response("allUpdate", None))
    backend.set_response(
        "CreateUpdateUpdate", mutation_response("CreateUpdateUpdate")
    )

    client.upsert_raw_source_update(
        "br_x", "tab", latest=datetime.date(2026, 6, 1)
    )

    v = _input(backend.mutation_for("CreateUpdateUpdate"))
    assert v["rawDataSource"] == RDS
    assert "table" not in v


def test_upsert_coverage_datetime_range_passes_dto_fields(client, backend):
    backend.set_response(
        "allDatetimerange", node_response("allDatetimerange", None)
    )
    backend.set_response(
        "CreateUpdateDateTimeRange",
        mutation_response("CreateUpdateDateTimeRange"),
    )

    dto = DateTimeRangeInput(coverage=UUID, endYear=2026, endMonth=6, endDay=1)
    client.upsert_coverage_datetime_range(dto)

    v = _input(backend.mutation_for("CreateUpdateDateTimeRange"))
    assert v["coverage"] == UUID
    assert v["endYear"] == 2026 and v["endMonth"] == 6 and v["endDay"] == 1


def test_write_carries_auth_header(client, backend):
    backend.set_response(
        "allRawdatasource", node_response("allRawdatasource", RDS)
    )
    backend.set_response("allPoll", node_response("allPoll", "poll-9"))
    backend.set_response(
        "CreateUpdatePoll", mutation_response("CreateUpdatePoll")
    )

    client.upsert_raw_source_poll(
        "br_x", "tab", latest=datetime.date(2026, 6, 1)
    )
    assert backend.mutation_for("CreateUpdatePoll")["headers"] == {
        "Authorization": "Bearer tok-fake"
    }


# ============================================================ TIPO 2 — response
def test_query_id_rejects_multiple_nodes(client, backend):  # R19
    # _raw_source_id → _query_id("allRawdatasource", ...); 2 nós devem abortar.
    backend.set_response(
        "allRawdatasource",
        {"allRawdatasource": {"items": [{"_id": "a"}, {"_id": "b"}]}},
    )
    with pytest.raises(ValueError, match="mais de um"):
        client.upsert_raw_source_poll(
            "br_x", "tab", latest=datetime.date(2026, 6, 1)
        )


def test_missing_update_returns_none(client, backend):
    backend.set_response("allUpdate", node_response("allUpdate", None))
    assert client.get_table_update_latest("br_x", "tab") is None


def test_update_latest_parsed_to_date(client, backend):
    backend.set_response(
        "allUpdate",
        {
            "allUpdate": {
                "items": [{"id": "u:1", "latest": "2026-06-01T10:00:00"}]
            }
        },
    )
    assert client.get_table_update_latest("br_x", "tab") == datetime.date(
        2026, 6, 1
    )


def test_mutation_errors_raise(client, backend):
    backend.set_response(
        "allRawdatasource", node_response("allRawdatasource", RDS)
    )
    backend.set_response("allPoll", node_response("allPoll", "poll-9"))
    backend.set_response(
        "CreateUpdatePoll",
        mutation_response(
            "CreateUpdatePoll",
            errors=[{"field": "latest", "messages": ["inválido"]}],
        ),
    )
    with pytest.raises(BackendMutationError):
        client.upsert_raw_source_poll(
            "br_x", "tab", latest=datetime.date(2026, 6, 1)
        )


# ============================================================ TIPO 3 — auth cache
def test_token_authenticated_once_per_instance(monkeypatch):
    monkeypatch.setattr(
        "pipelines.utils.metadata.client.get_credentials_from_secret",
        lambda secret_path: {"email": "e", "password": "p"},
    )
    backend = RecordingBackend()
    backend.set_response("tokenAuth", {"tokenAuth": {"token": "live-token"}})
    backend.set_response(
        "allRawdatasource", node_response("allRawdatasource", "rds-1")
    )
    backend.set_response("allPoll", node_response("allPoll", "poll-9"))
    backend.set_response("allUpdate", node_response("allUpdate", "upd-9"))
    backend.set_response(
        "CreateUpdatePoll", mutation_response("CreateUpdatePoll")
    )
    backend.set_response(
        "CreateUpdateUpdate", mutation_response("CreateUpdateUpdate")
    )

    client = MetadataClient(env="dev", backend=backend)
    client.upsert_raw_source_poll(
        "br_x", "tab", latest=datetime.date(2026, 6, 1)
    )
    client.upsert_raw_source_update(
        "br_x", "tab", latest=datetime.date(2026, 6, 1)
    )

    token_calls = [c for c in backend.calls if "tokenAuth" in c["query"]]
    assert (
        len(token_calls) == 1
    )  # §1.6: o código antigo re-autenticava por mutation


def test_invalid_env_rejected():
    with pytest.raises(ValueError, match="env inválido"):
        # pyrefly: ignore [bad-argument-type]
        MetadataClient(env="production")
