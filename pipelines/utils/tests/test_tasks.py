"""
Testes de `pipelines/utils/tasks.py::_resolve_dbt_model_path`.

A maioria dos modelos do repositório nomeia o arquivo
`{dataset_id}__{table_id}.sql`; uma minoria usa `{table_id}.sql` direto.
Em vez de depender de um `dbt_alias` declarado pelo chamador (fonte
recorrente de bugs quando calculado errado — ver incidente documentado em
`br_bcb_sicor/flows.py`), a função detecta qual dos dois existe de verdade.
"""

from pathlib import Path

from pipelines.utils.tasks import _resolve_dbt_model_path


def _touch(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("select 1")


def test_resolve_dbt_model_path_prefers_aliased(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _touch(tmp_path / "models" / "ds" / "ds__tb.sql")

    selected = _resolve_dbt_model_path("ds", "tb")

    assert selected == Path("models") / "ds" / "ds__tb.sql"


def test_resolve_dbt_model_path_falls_back_to_plain(tmp_path, monkeypatch):
    """Caso dos 54 modelos do repositório fora da convenção de prefixo (ex.
    `br_bd_metadados`)."""
    monkeypatch.chdir(tmp_path)
    _touch(tmp_path / "models" / "ds" / "tb.sql")

    selected = _resolve_dbt_model_path("ds", "tb")

    assert selected == Path("models") / "ds" / "tb.sql"


def test_resolve_dbt_model_path_prefers_aliased_when_both_exist(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    _touch(tmp_path / "models" / "ds" / "ds__tb.sql")
    _touch(tmp_path / "models" / "ds" / "tb.sql")

    selected = _resolve_dbt_model_path("ds", "tb")

    assert selected == Path("models") / "ds" / "ds__tb.sql"


def test_resolve_dbt_model_path_neither_exists_keeps_aliased(
    tmp_path, monkeypatch
):
    """Nenhum dos dois existe — devolve o caminho com prefixo, pra `run_dbt`
    levantar o `FileNotFoundError` de sempre."""
    monkeypatch.chdir(tmp_path)

    selected = _resolve_dbt_model_path("ds", "tb")

    assert selected == Path("models") / "ds" / "ds__tb.sql"
