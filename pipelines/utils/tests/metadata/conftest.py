"""
Infraestrutura de testes da camada de metadados (Data Basis backend).

Contém os *test doubles* compartilhados pelos tipos 1-4 do plano de testagem
(`refatorar_backend.md` §4):

- `RecordingBackend` — grava toda tupla `(query, variables, headers)` emitida.
  Injeta-se em `MetadataClient._execute` (a costura única de transporte).
  Alimenta os testes de request-contract (tipo 1), response-handling (tipo 2)
  e auth (tipo 3).
- `FakeMetadataClient` — double in-memory que grava quais `upsert_*` foram
  chamados, com quais DTOs e em que ordem. Alimenta os testes de orquestrador
  (tipo 4), onde a matriz de escrita da §1.8 vira asserção.

Nenhuma destas classes importa `MetadataClient` — elas existem para que os
módulos `client.py`/`tasks.py` (M2/M3) sejam testáveis assim que nascerem,
sem que este conftest dependa deles.
"""

from __future__ import annotations

import pytest


def node_response(query_class: str, _id: str | None) -> dict:
    """Resposta no shape que `bd.Backend._execute_query` devolve (edges→items)."""
    items = [{"_id": _id}] if _id is not None else []
    return {query_class: {"items": items}}


def mutation_response(
    mutation_class: str, _id: str = "x:123", errors=None
) -> dict:
    _classe = mutation_class.replace("CreateUpdate", "").lower()
    return {mutation_class: {"errors": errors, _classe: {"id": _id}}}


class RecordingBackend:
    """Stand-in de `bd.Backend` injetável em `MetadataClient(backend=...)`.

    Grava toda chamada `(query, variables, headers)` e devolve respostas
    roteirizadas por substring da query (ex.: `"CreateUpdatePoll"`). Implementa
    a mesma superfície que o cliente usa: `_execute_query` e
    `_get_table_id_from_name`.
    """

    def __init__(
        self,
        responses: dict | None = None,
        table_pk: str = "22222222-2222-4222-8222-222222222222",
    ):
        self.calls: list[dict] = []
        self.table_pk = table_pk
        self._responses = dict(responses or {})

    def _execute_query(
        self,
        query: str,
        variables: dict | None = None,
        headers: dict | None = None,
    ):
        self.calls.append(
            {
                "query": query,
                "variables": variables or {},
                "headers": headers or {},
            }
        )
        for key, resp in self._responses.items():
            if key in query:
                return resp
        return {}

    # alias histórico usado em alguns testes
    execute = _execute_query

    def _get_table_id_from_name(
        self, gcp_dataset_id: str, gcp_table_id: str
    ) -> str:
        self.calls.append(
            {
                "query": "_get_table_id_from_name",
                "dataset": gcp_dataset_id,
                "table": gcp_table_id,
            }
        )
        return self.table_pk

    def set_response(self, needle: str, resp: dict) -> RecordingBackend:
        self._responses[needle] = resp
        return self

    def calls_matching(self, needle: str) -> list[dict]:
        return [c for c in self.calls if needle in c["query"]]

    def mutation_for(self, mutation_class: str) -> dict:
        hits = [
            c
            for c in self.calls
            if f"{mutation_class}(input:"
            in c["query"].replace("\n", " ").replace("  ", " ")
            or f"{mutation_class}(input" in c["query"]
        ]
        if not hits:  # fallback robusto a espaços
            hits = [
                c
                for c in self.calls
                if mutation_class in c["query"] and "mutation(" in c["query"]
            ]
        assert len(hits) == 1, (
            f"esperava 1 mutation {mutation_class}, vi {len(hits)}"
        )
        return hits[0]


class FakeMetadataClient:
    """Double in-memory de `MetadataClient`. Grava as escritas em ordem.

    Os testes de orquestrador assertam `[w[0] for w in client.writes]` contra a
    matriz §1.8 — incluindo o negativo (que entidades *não* foram tocadas).
    """

    def __init__(
        self,
        raw_source_update_latest=None,
        table_update_latest=None,
        table_coverage_end=None,
        table_status="under_review",
        coverage_ids=None,
    ):
        self.writes: list[tuple] = []
        self._raw_source_update_latest = raw_source_update_latest
        self._table_update_latest = table_update_latest
        self._table_coverage_end = table_coverage_end
        self._table_status = table_status
        self._coverage_ids = coverage_ids

    # --- escrita (uma por entidade) -------------------------------------------
    def upsert_raw_source_poll(self, *args, **kwargs):
        self.writes.append(("poll", args, kwargs))

    def upsert_raw_source_update(self, *args, **kwargs):
        self.writes.append(("raw_source_update", args, kwargs))

    def upsert_table_update(self, *args, **kwargs):
        self.writes.append(("table_update", args, kwargs))

    def upsert_coverage_datetime_range(self, *args, **kwargs):
        self.writes.append(("coverage", args, kwargs))

    # --- leitura --------------------------------------------------------------
    def get_raw_source_update_latest(self, *_args, **_kwargs):
        return self._raw_source_update_latest

    def get_table_update_latest(self, *_args, **_kwargs):
        return self._table_update_latest

    def get_table_coverage_end(self, *_args, **_kwargs):
        return self._table_coverage_end

    def get_table_status(self, *_args, **_kwargs):
        return self._table_status

    def get_coverage_ids(self, *_args, **_kwargs):
        from pipelines.utils.metadata.policy import CoverageIds

        return self._coverage_ids or CoverageIds()

    # --- helpers de asserção --------------------------------------------------
    @property
    def written_entities(self) -> list[str]:
        return [w[0] for w in self.writes]


class FakeBQ:
    """Adapter de BigQuery in-memory para testar `register_table_materialization`."""

    def __init__(
        self,
        max_date=None,
        last_modified=None,
        can_read=True,
    ):
        self._max_date = max_date
        self._last_modified = last_modified
        self._can_read = can_read
        self.rap_calls: list[tuple] = []

    def read_max_date(self, dataset_id, table_id, coverage):
        return self._max_date

    def last_modified(self, dataset_id, table_id):
        return self._last_modified

    def can_read_metadata(self, bq_project):
        return self._can_read

    def apply_row_access_policies(
        self, coverage, free_end, dataset_id, table_id
    ):
        self.rap_calls.append((coverage, free_end, dataset_id, table_id))


@pytest.fixture
def backend() -> RecordingBackend:
    return RecordingBackend()


@pytest.fixture
def client(backend):
    """MetadataClient com transporte gravável, Entity injetada e token fixo.

    Os testes que exercitam autenticação NÃO usam esta fixture (querem observar
    o tokenAuth); usam `MetadataClient(backend=...)` direto.
    """
    from pipelines.utils.metadata.client import MetadataClient

    c = MetadataClient(
        env="dev",
        backend=backend,
        entity_ids={
            "day": "00000000-0000-4000-8000-0000000000d1",
            "month": "00000000-0000-4000-8000-0000000000e2",
        },
    )
    c._token = "tok-fake"  # pré-autenticado: pula o tokenAuth
    return c


@pytest.fixture
def fake_client() -> FakeMetadataClient:
    return FakeMetadataClient()
