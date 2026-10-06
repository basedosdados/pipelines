"""Testes do guarda-chuva fail-closed do export open de tabelas part_bdpro.

Regressão para o vazamento em que `dbt run` (CREATE OR REPLACE TABLE) apaga as
row access policies e o export open subsequente publica a janela BD Pro inteira.
"""

from __future__ import annotations

from typing import Any

import pytest
from google.api_core.exceptions import NotFound

from pipelines.utils import tasks as t


class _FakeJob:
    """Job do BigQuery que termina imediatamente."""

    def done(self) -> bool:
        return True

    def result(self) -> None:
        return None


class _FakeBQ:
    """Client do BigQuery cujo DROP de policy levanta NotFound."""

    def __init__(self, num_bytes: int) -> None:
        self._num_bytes = num_bytes

    def get_table(self, _ref: Any) -> Any:
        class _T:
            num_bytes = self._num_bytes
            num_rows = 10

        return _T()

    def query(self, sql: str) -> _FakeJob:
        if "DROP ROW ACCESS POLICY" in sql:
            raise NotFound("no policy")
        return _FakeJob()


@pytest.fixture
def wiring(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Instrumenta o export e devolve a lista de destinos exportados."""
    exported: list[str] = []

    monkeypatch.setattr(
        t, "_google_client", lambda *a, **k: {"bigquery": _FakeBQ(1_000)}
    )
    monkeypatch.setattr(
        t,
        "get_credentials_from_secret",
        lambda _p: {
            "URL_DOWNLOAD_OPEN": "gs://open/",
            "URL_DOWNLOAD_CLOSED": "gs://closed/",
        },
    )
    monkeypatch.setattr(
        t,
        "_execute_query_in_bigquery",
        lambda _b, _q, path, _l=None: exported.append(path),
    )
    return exported


def test_paywalled_table_without_policy_skips_open_export(
    monkeypatch: pytest.MonkeyPatch, wiring: list[str]
) -> None:
    """Coverage pro + policy ausente: exporta só BDPro, nunca o caminho open."""
    monkeypatch.setattr(t, "_table_expects_bdpro_paywall", lambda _d, _t: True)

    t.download_data_to_gcs.fn(dataset_id="ds", table_id="tb")

    assert wiring == ["gs://closed/ds/tb/tb_bdpro.csv.gz"]
    assert not any("open" in p for p in wiring)


def test_open_table_without_policy_still_exports_open(
    monkeypatch: pytest.MonkeyPatch, wiring: list[str]
) -> None:
    """Sem Coverage pro a tabela é aberta de verdade: export open preservado."""
    monkeypatch.setattr(
        t, "_table_expects_bdpro_paywall", lambda _d, _t: False
    )

    t.download_data_to_gcs.fn(dataset_id="ds", table_id="tb")

    assert wiring == ["gs://open/ds/tb/tb.csv.gz"]


def test_backend_failure_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Erro ao consultar o backend assume paywall em vez de vazar."""

    def _boom(*_a: Any, **_k: Any) -> Any:
        raise RuntimeError("backend down")

    monkeypatch.setattr(t.bd, "Backend", _boom)

    assert t._table_expects_bdpro_paywall("ds", "tb") is True


class _FakeBackend(t.bd.Backend):
    """Backend que devolve uma resposta GraphQL crua, simplificada pelo SDK.

    Usa o `_simplify_response` real, para que o teste veja o mesmo formato que
    `_execute_query` devolve em produção (`edges` vira `{"items": [...]}`).
    """

    instances: list[_FakeBackend] = []

    def __init__(
        self,
        raw: dict[str, Any],
        django_table_id: str | None = "uuid",
        **kwargs: Any,
    ) -> None:
        self._raw = raw
        self._django_table_id = django_table_id
        self.kwargs = kwargs
        _FakeBackend.instances.append(self)

    def _get_table_id_from_name(
        self, gcp_dataset_id: str, gcp_table_id: str
    ) -> str | None:
        return self._django_table_id

    def _execute_query(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
        client: Any = None,
        headers: dict[str, str] | None = None,
        page: int = 1,
        page_size: int = 10,
        fetch_schema_from_transport: bool = False,
    ) -> dict[str, Any]:
        return self._simplify_response(self._raw)


def _coverages_response(*is_closed: bool) -> dict[str, Any]:
    """Resposta crua de `allTable { coverages { isClosed } }`."""
    return {
        "allTable": {
            "edges": [
                {
                    "node": {
                        "coverages": {
                            "edges": [
                                {"node": {"isClosed": closed}}
                                for closed in is_closed
                            ]
                        }
                    }
                }
            ]
        }
    }


@pytest.mark.parametrize(
    ("is_closed", "expected"),
    [
        ((False, True), True),  # coverage aberta + coverage pro
        ((False,), False),  # só coverage aberta
    ],
)
def test_paywall_reads_coverages_in_sdk_response_shape(
    monkeypatch: pytest.MonkeyPatch,
    is_closed: tuple[bool, ...],
    expected: bool,
) -> None:
    """Lê `isClosed` no formato que o SDK devolve, sem cair no fail closed."""
    monkeypatch.setattr(
        t.bd,
        "Backend",
        lambda **k: _FakeBackend(_coverages_response(*is_closed), **k),
    )

    assert t._table_expects_bdpro_paywall("ds", "tb") is expected


def test_paywall_uses_backend_graphql_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Consulta o backend de prod, não `api.basedosdados.org`."""
    _FakeBackend.instances.clear()
    monkeypatch.setattr(
        t.bd,
        "Backend",
        lambda **k: _FakeBackend(_coverages_response(False), **k),
    )

    t._table_expects_bdpro_paywall("ds", "tb")

    assert _FakeBackend.instances[0].kwargs == {
        "graphql_url": t.constants.API_URL.value["prod"]
    }


def test_table_missing_from_backend_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tabela sem registro no backend assume paywall."""
    monkeypatch.setattr(
        t.bd,
        "Backend",
        lambda **k: _FakeBackend({}, django_table_id=None, **k),
    )

    assert t._table_expects_bdpro_paywall("ds", "tb") is True
