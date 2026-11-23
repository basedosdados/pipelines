"""Testes do limite de falhas de `_fetch_many`, sem rede."""

from __future__ import annotations

from collections.abc import Callable, Iterable

import pytest

from pipelines.datasets.br_senado_dados_abertos import utils


def _fake_get_json(
    failing: Iterable[str] = (), not_found: Iterable[str] = ()
) -> Callable[..., dict]:
    """Monta um `get_json` falso que falha ou responde 404 nos caminhos dados.

    Args:
        failing: caminhos que falham em todas as tentativas.
        not_found: caminhos que respondem 404.

    Returns:
        Função com a assinatura de `get_json`.
    """
    failing_paths = set(failing)
    not_found_paths = set(not_found)

    def fake(
        path: str, retries: int = 6, fail_fast_on_404: bool = False
    ) -> dict:
        """Responde como `get_json`, com falha ou 404 nos caminhos dados."""
        if path in not_found_paths:
            if fail_fast_on_404:
                raise utils.NotFoundError(f"GET {path}: HTTP 404")
            raise RuntimeError(f"GET {path} failed: HTTP 404")
        if path in failing_paths:
            raise RuntimeError("connection reset")
        return {"path": path}

    return fake


def test_failures_at_the_limit_return_with_none(monkeypatch, capsys):
    """Falhas em até 1% das chamadas viram aviso, com None no lugar."""
    paths = [f"/senador/{code}/mandatos" for code in range(100)]
    monkeypatch.setattr(utils, "get_json", _fake_get_json(failing=[paths[7]]))

    results = utils._fetch_many(paths)

    assert len(results) == 100
    assert results[7] is None
    assert results[8] == {"path": paths[8]}
    assert "1/100 requests failed after retries" in capsys.readouterr().out


def test_failures_above_the_limit_raise(monkeypatch):
    """Falhas acima de 1% das chamadas levantam erro."""
    paths = [f"/senador/{code}/mandatos" for code in range(100)]
    monkeypatch.setattr(
        utils, "get_json", _fake_get_json(failing=[paths[7], paths[42]])
    )

    with pytest.raises(RuntimeError, match="2/100 requests failed"):
        utils._fetch_many(paths)


def test_404_does_not_count_as_failure_with_not_found_ok(monkeypatch, capsys):
    """Com `not_found_ok`, o 404 vira resposta sem registros, fora da conta."""
    paths = [f"/votacaoComissao/comissao/C{code}" for code in range(100)]
    monkeypatch.setattr(
        utils, "get_json", _fake_get_json(not_found=paths[:10])
    )

    results = utils._fetch_many(paths, not_found_ok=True)

    assert results[:10] == [None] * 10
    assert results[10] == {"path": paths[10]}
    assert "10/100 requests answered HTTP 404" in capsys.readouterr().out


def test_404_counts_as_failure_without_not_found_ok(monkeypatch):
    """Sem `not_found_ok`, o 404 conta como falha para o limite."""
    paths = [f"/senador/{code}/cargos" for code in range(100)]
    monkeypatch.setattr(
        utils, "get_json", _fake_get_json(not_found=paths[:10])
    )

    with pytest.raises(RuntimeError, match="10/100 requests failed"):
        utils._fetch_many(paths)
