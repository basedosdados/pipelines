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


# ── Consulta ao backend ──────────────────────────────────────────────────────
# Regressão da run 9d7bbd3b (2026-10-19): `api.basedosdados.org` deixou de ser
# o GraphQL do backend, e a resposta do `bd.Backend` chega simplificada
# (`edges` → `items` também nas Coverages aninhadas). Os dois faziam a checagem
# cair sempre no fail closed — e toda tabela aberta < 100 MB parar de
# atualizar seu arquivo open.


def _raw_coverages(*is_closed: bool) -> dict:
    return {
        "allTable": {
            "edges": [
                {
                    "node": {
                        "coverages": {
                            "edges": [
                                {"node": {"isClosed": c}} for c in is_closed
                            ]
                        }
                    }
                }
            ]
        }
    }


# Instanciado no import, antes de qualquer monkeypatch de `bd.Backend`.
_REAL_BACKEND = t.bd.Backend(graphql_url="http://unused")


def _simplified(raw: dict) -> dict:
    """Passa a resposta pelo mesmo `_simplify_response` do `bd.Backend`."""
    return _REAL_BACKEND._simplify_response(raw)


def test_backend_url_is_the_graphql_backend() -> None:
    assert (
        t.BACKEND_GRAPHQL_URL
        == "https://backend.basedosdados.org/api/v1/graphql"
    )


@pytest.mark.parametrize(
    ("is_closed", "expected"),
    [((True, False), True), ((False,), False), ((), False)],
)
def test_has_pro_coverage_reads_simplified_response(
    is_closed: tuple[bool, ...], expected: bool
) -> None:
    assert t._has_pro_coverage(_simplified(_raw_coverages(*is_closed))) is (
        expected
    )


def test_has_pro_coverage_table_missing() -> None:
    assert (
        t._has_pro_coverage(_simplified({"allTable": {"edges": []}})) is None
    )


@pytest.mark.parametrize(
    ("is_closed", "expected"), [((True, False), True), ((False,), False)]
)
def test_paywall_check_uses_backend_and_parses_coverages(
    monkeypatch: pytest.MonkeyPatch,
    is_closed: tuple[bool, ...],
    expected: bool,
) -> None:
    """Tabela aberta precisa sair False, não cair no fail closed."""
    urls: list[str] = []

    class _FakeBackend:
        def __init__(self, graphql_url: str) -> None:
            urls.append(graphql_url)

        def _get_table_id_from_name(self, **_k: Any) -> str:
            return "uuid"

        def _execute_query(self, _q: str) -> dict:
            return _simplified(_raw_coverages(*is_closed))

    monkeypatch.setattr(t.bd, "Backend", _FakeBackend)

    assert t._table_expects_bdpro_paywall("ds", "tb") is expected
    assert urls == [t.BACKEND_GRAPHQL_URL]


# ── Decisão de tier do one-click download ────────────────────────────────────


@pytest.mark.parametrize(
    ("num_bytes", "paywalled", "expected"),
    [
        (2_000_000_000, False, []),
        (2_000_000_000, True, []),
        (500_000_000, False, ["gs://closed/ds/tb/tb_bdpro.csv.gz"]),
        (500_000_000, True, ["gs://closed/ds/tb/tb_bdpro.csv.gz"]),
        (1_000, False, ["gs://open/ds/tb/tb.csv.gz"]),
        # paywalled < 100 MB: o open é regravado depois das policies
        (1_000, True, ["gs://closed/ds/tb/tb_bdpro.csv.gz"]),
    ],
)
def test_tier_decision_without_policy(
    monkeypatch: pytest.MonkeyPatch,
    wiring: list[str],
    num_bytes: int,
    paywalled: bool,
    expected: list[str],
) -> None:
    monkeypatch.setattr(
        t, "_google_client", lambda *a, **k: {"bigquery": _FakeBQ(num_bytes)}
    )
    monkeypatch.setattr(
        t, "_table_expects_bdpro_paywall", lambda _d, _t: paywalled
    )

    t.download_data_to_gcs.fn(dataset_id="ds", table_id="tb")

    assert wiring == expected


@pytest.fixture
def queries(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Instrumenta o export e devolve (query, destino) de cada export."""
    exported: list[tuple[str, str]] = []
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
        lambda _b, q, path, _l=None: exported.append((q, path)),
    )
    return exported


def test_free_rows_export_filters_on_free_end(
    monkeypatch: pytest.MonkeyPatch, queries: list[tuple[str, str]]
) -> None:
    """O open de uma tabela paywalled leva só as linhas free, filtradas na query."""
    monkeypatch.setattr(
        t, "_google_client", lambda *a, **k: {"bigquery": _FakeBQ(1_000)}
    )

    t.export_free_rows_to_open(
        dataset_id="ds",
        table_id="tb",
        free_filter='DATE(ano,mes,1)<="2026-3-01"',
    )

    assert queries == [
        (
            'SELECT * FROM `basedosdados.ds.tb` WHERE DATE(ano,mes,1)<="2026-3-01"',
            "gs://open/ds/tb/tb.csv.gz",
        )
    ]


@pytest.mark.parametrize("num_bytes", [0, 100_000_000, 2_000_000_000])
def test_free_rows_export_skips_outside_open_tier(
    monkeypatch: pytest.MonkeyPatch,
    queries: list[tuple[str, str]],
    num_bytes: int,
) -> None:
    monkeypatch.setattr(
        t, "_google_client", lambda *a, **k: {"bigquery": _FakeBQ(num_bytes)}
    )

    t.export_free_rows_to_open(dataset_id="ds", table_id="tb", free_filter="x")

    assert queries == []
