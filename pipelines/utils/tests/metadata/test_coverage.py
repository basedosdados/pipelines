"""
Testes de `pipelines/utils/metadata/coverage.py` — protótipo de CoverageSpec
vindo do backend (ver docstring do módulo).
"""

import pytest

from pipelines.utils.metadata.coverage import (
    CoverageSpec,
    coverage_from_backend,
)


class _FakeClient:
    """Double mínimo de `MetadataClient` — só o método que `coverage_from_backend`
    chama."""

    def __init__(self, raw: dict | None):
        self._raw = raw
        self.calls: list[tuple[str, str]] = []

    def get_coverage_spec_raw(
        self, dataset_id: str, table_id: str
    ) -> dict | None:
        self.calls.append((dataset_id, table_id))
        return self._raw


def test_coverage_from_backend_builds_spec_from_raw_response():
    client = _FakeClient(
        {
            "tier": "all_free",
            "date_column_kind": "year_quarter",
            "coverage_columns": [
                {"name": "ano", "entity": "year"},
                {"name": "trimestre", "entity": "quarter"},
            ],
        }
    )

    # pyrefly: ignore [bad-argument-type]
    spec = coverage_from_backend(client, "test_dataset", "test_event_pipeline")

    assert client.calls == [("test_dataset", "test_event_pipeline")]
    assert spec.tier == "all_free"
    assert spec.date_column_kind == "year_quarter"
    assert spec.column("year") == "ano"
    assert spec.column("quarter") == "trimestre"


def test_coverage_from_backend_raises_when_table_not_found():
    client = _FakeClient(None)

    with pytest.raises(ValueError, match="Backend não tem coverageSpec"):
        # pyrefly: ignore [bad-argument-type]
        coverage_from_backend(client, "test_dataset", "test_event_pipeline")


def test_coverage_from_backend_raises_when_tier_is_empty():
    client = _FakeClient(
        {
            "tier": "",
            "date_column_kind": "",
            "coverage_columns": [{"name": "ano", "entity": "year"}],
        }
    )

    with pytest.raises(ValueError, match="nunca onboardada"):
        # pyrefly: ignore [bad-argument-type]
        coverage_from_backend(client, "test_dataset", "test_event_pipeline")


def test_coverage_spec_non_historical_has_no_coverage_columns():
    spec = CoverageSpec(
        tier="non_historical", date_column_kind="", coverage_columns=[]
    )

    assert spec.coverage_columns == []
    with pytest.raises(StopIteration):
        spec.column("year")
