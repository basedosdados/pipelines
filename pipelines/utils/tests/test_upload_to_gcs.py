"""Testes de integração de `_upload_to_gcs` e `upload_to_gcs`.

Só o que cruza a rede é falso (`bd.Table`, `bd.Storage` e o client do BigQuery).
Arquivos, schemas e regras de `schema_check` são reais. Todos os mocks ficam
anexados a um mock pai, que registra a ordem global das chamadas.
"""

from unittest.mock import MagicMock, create_autospec

import basedosdados as bd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from google.api_core.exceptions import NotFound, ServiceUnavailable
from prefect import flow
from prefect.states import Completed
from prefect.testing.utilities import prefect_test_harness

from pipelines.utils import tasks
from pipelines.utils.schema_check import (
    Finding,
    SchemaCheckResult,
    SchemaMismatchError,
    Severity,
)
from pipelines.utils.tasks import _upload_to_gcs, upload_to_gcs
from pipelines.utils.tests.test_schema_check import _bq_table

ARGS = {"dataset_id": "ds", "table_id": "tb", "bucket_name": "bkt"}


# Fixtures
@pytest.fixture
def gcp(monkeypatch):
    """Substitui as bordas de rede e devolve o mock pai (ordem em `mock_calls`)."""
    parent = MagicMock()
    tb = create_autospec(bd.Table, instance=True)
    st = create_autospec(bd.Storage, instance=True)
    client = MagicMock()
    parent.attach_mock(tb, "tb")
    parent.attach_mock(st, "st")
    parent.attach_mock(client, "client")

    tb.client = {"bigquery_staging": client}
    tb.table_full_name = {"staging": "proj.ds.tb"}
    tb.table_exists.return_value = True

    monkeypatch.setattr(tasks.bd, "Table", lambda **kw: tb)
    monkeypatch.setattr(tasks.bd, "Storage", lambda **kw: st)
    monkeypatch.setattr(tasks, "dump_header", lambda **kw: "header")
    return parent


def staging_table(names, keys=None):
    definition = (
        {"partitionedColumn": [{"field": k} for k in keys]} if keys else None
    )
    return _bq_table([(n, "STRING") for n in names], definition, bool(keys))


def write_csv(path, cols):
    path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({c: ["x"] for c in cols}).to_csv(
        path / "data.csv", index=False
    )
    return path


def write_parquet(path, columns: dict):
    path.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path / "data.parquet")
    return path


# _upload_to_gcs: append
def test_append_sem_staging_cria_e_nao_checa(gcp, tmp_path):
    data = write_csv(tmp_path / "d", ["a", "b"])
    state = {"exists": False}
    gcp.tb.table_exists.side_effect = lambda **kw: state["exists"]
    gcp.tb.create.side_effect = lambda **kw: state.update(exists=True)

    result = _upload_to_gcs(data, **ARGS)

    assert result is None
    gcp.tb.create.assert_called_once()
    gcp.client.get_table.assert_not_called()  # primeiro upload: sem comparação
    gcp.st.upload.assert_called_once()


def test_append_sem_mudancas_sobe_sem_alterar_schema(gcp, tmp_path):
    data = write_csv(tmp_path / "d", ["a", "b"])
    gcp.client.get_table.return_value = staging_table(["a", "b"])

    result = _upload_to_gcs(data, **ARGS)

    # pyrefly: ignore [missing-attribute]
    assert not result.has_changes
    gcp.client.update_table.assert_not_called()
    gcp.st.upload.assert_called_once()


def test_append_com_erro_nao_altera_nada(gcp, tmp_path):
    data = write_csv(tmp_path / "d", ["b", "a"])  # ordem trocada em CSV
    gcp.client.get_table.return_value = staging_table(["a", "b"])

    with pytest.raises(SchemaMismatchError):
        _upload_to_gcs(data, **ARGS)

    gcp.client.update_table.assert_not_called()
    gcp.st.upload.assert_not_called()
    gcp.st.delete_table.assert_not_called()
    gcp.tb.delete.assert_not_called()
    gcp.tb.create.assert_not_called()


def test_append_coluna_nova_atualiza_schema_antes_do_upload(gcp, tmp_path):
    data = write_csv(tmp_path / "d", ["a", "b", "c"])
    table = staging_table(["a", "b"])
    gcp.client.get_table.return_value = table

    result = _upload_to_gcs(data, **ARGS)

    # pyrefly: ignore [missing-attribute]
    assert result.warnings and not result.errors
    gcp.client.update_table.assert_called_once_with(table, ["schema"])
    assert [f.name for f in table.schema][-1] == "c"
    assert (
        table.schema[-1].field_type == "STRING"
    )  # CSV: sem tipo, vira STRING
    names = [c[0] for c in gcp.mock_calls]
    assert names.index("client.update_table") < names.index("st.upload")


def test_append_parquet_coluna_nova_usa_tipo_do_arquivo(gcp, tmp_path):
    data = write_parquet(
        tmp_path / "d",
        {
            "a": pa.array(["x"], pa.string()),
            "n": pa.array([1], pa.int64()),
        },
    )
    table = staging_table(["a"])
    gcp.client.get_table.return_value = table

    _upload_to_gcs(data, source_format="parquet", **ARGS)

    novo = table.schema[-1]
    assert (novo.name, novo.field_type) == ("n", "INT64")


def test_append_parquet_tipo_incompativel_e_erro(gcp, tmp_path):
    data = write_parquet(tmp_path / "d", {"a": pa.array([1], pa.int64())})
    gcp.client.get_table.return_value = staging_table(["a"])  # STRING

    with pytest.raises(SchemaMismatchError, match="a \\(STRING -> INT64\\)"):
        _upload_to_gcs(data, source_format="parquet", **ARGS)
    gcp.st.upload.assert_not_called()


def test_append_particionado_ignora_chaves_na_comparacao(gcp, tmp_path):
    data = write_csv(tmp_path / "d" / "ano=2024", ["a", "b"]).parent
    # `table.schema` do BigQuery traz a chave de partição no fim.
    gcp.client.get_table.return_value = staging_table(
        ["a", "b", "ano"], keys=["ano"]
    )

    result = _upload_to_gcs(data, **ARGS)

    # pyrefly: ignore [missing-attribute]
    assert not result.has_changes
    gcp.st.upload.assert_called_once()


def test_append_layout_de_particao_diferente_e_erro(gcp, tmp_path):
    data = write_csv(tmp_path / "d" / "mes=01", ["a"]).parent
    gcp.client.get_table.return_value = staging_table(
        ["a", "ano"], keys=["ano"]
    )

    with pytest.raises(SchemaMismatchError, match="layout de partição"):
        _upload_to_gcs(data, **ARGS)
    gcp.st.upload.assert_not_called()


def test_append_staging_some_durante_a_checagem(gcp, tmp_path):
    data = write_csv(tmp_path / "d", ["a"])
    gcp.client.get_table.side_effect = NotFound("sumiu")

    with pytest.raises(RuntimeError, match="deixou de existir"):
        _upload_to_gcs(data, **ARGS)
    gcp.st.upload.assert_not_called()


# _upload_to_gcs: fail-closed
@pytest.mark.parametrize("dump_mode", ["append", "overwrite"])
def test_falha_da_checagem_aborta_e_nao_e_schema_mismatch(
    gcp, tmp_path, dump_mode
):
    data = write_csv(tmp_path / "d", ["a"])
    gcp.client.get_table.side_effect = ServiceUnavailable("bq fora")

    with pytest.raises(ServiceUnavailable) as exc:
        _upload_to_gcs(data, dump_mode=dump_mode, **ARGS)

    assert not isinstance(exc.value, SchemaMismatchError)  # mantém o retry
    gcp.st.upload.assert_not_called()
    gcp.st.delete_table.assert_not_called()
    gcp.tb.delete.assert_not_called()


# _upload_to_gcs: overwrite
def test_overwrite_le_schema_antes_de_deletar(gcp, tmp_path):
    data = write_csv(tmp_path / "d", ["a", "b"])
    gcp.client.get_table.return_value = staging_table(["a", "b"])

    _upload_to_gcs(data, dump_mode="overwrite", **ARGS)

    names = [c[0] for c in gcp.mock_calls]
    leitura = names.index("client.get_table")
    assert leitura < names.index("st.delete_table")
    assert leitura < names.index("tb.delete")
    assert names.index("tb.delete") < names.index("tb.create")
    assert names.index("tb.create") < names.index("st.upload")


def test_overwrite_com_divergencia_nao_bloqueia_so_avisa(gcp, tmp_path):
    data = write_csv(tmp_path / "d", ["b", "a"])  # em append seria erro
    gcp.client.get_table.return_value = staging_table(["a", "b"])

    result = _upload_to_gcs(data, dump_mode="overwrite", **ARGS)

    # pyrefly: ignore [missing-attribute]
    assert not result.errors
    # pyrefly: ignore [missing-attribute]
    assert result.warnings
    gcp.tb.create.assert_called_once()
    gcp.st.upload.assert_called_once()
    gcp.client.update_table.assert_not_called()  # a tabela é recriada, não alterada


def test_overwrite_sem_staging_previa_nao_checa(gcp, tmp_path):
    data = write_csv(tmp_path / "d", ["a"])
    state = {"exists": False}
    gcp.tb.table_exists.side_effect = lambda **kw: state["exists"]
    gcp.tb.create.side_effect = lambda **kw: state.update(exists=True)

    result = _upload_to_gcs(data, dump_mode="overwrite", **ARGS)

    assert result is None
    gcp.client.get_table.assert_not_called()
    gcp.tb.delete.assert_not_called()
    gcp.tb.create.assert_called_once()


def test_overwrite_sem_mudancas_nao_avisa(gcp, tmp_path):
    data = write_csv(tmp_path / "d", ["a"])
    gcp.client.get_table.return_value = staging_table(["a"])

    result = _upload_to_gcs(data, dump_mode="overwrite", **ARGS)

    # pyrefly: ignore [missing-attribute]
    assert not result.has_changes


# _upload_to_gcs: entrada inválida
def test_dump_mode_invalido_nao_tem_efeito_colateral(gcp, tmp_path):
    data = write_csv(tmp_path / "d", ["a"])

    with pytest.raises(ValueError, match="dump_mode inválido"):
        _upload_to_gcs(data, dump_mode="merge", **ARGS)

    gcp.st.upload.assert_not_called()
    gcp.tb.create.assert_not_called()
    gcp.tb.delete.assert_not_called()


# Task upload_to_gcs: tradução do resultado
def _result(*findings):
    return SchemaCheckResult(findings=tuple(findings), added_columns=())


def test_task_devolve_schema_warning_quando_ha_avisos(monkeypatch):
    result = _result(Finding(Severity.WARNING, "colunas novas: c"))
    monkeypatch.setattr(tasks, "_upload_to_gcs", lambda **kw: result)

    state = upload_to_gcs.fn("x", **ARGS)

    assert isinstance(state, type(Completed()))
    assert state.name == "SchemaWarning"
    # pyrefly: ignore [not-iterable]
    assert "colunas novas: c" in state.message
    # pyrefly: ignore [not-iterable]
    assert "ds.tb" in state.message  # título com dataset.tabela


@pytest.mark.parametrize("result", [None, _result()])
def test_task_devolve_none_sem_avisos(monkeypatch, result):
    monkeypatch.setattr(tasks, "_upload_to_gcs", lambda **kw: result)
    assert upload_to_gcs.fn("x", **ARGS) is None


# Task upload_to_gcs: retry com o engine do Prefect
@pytest.fixture(scope="module")
def prefect_harness():
    with prefect_test_harness():
        yield


def _run_task_in_flow(monkeypatch, error):
    chamadas = MagicMock(side_effect=error)
    monkeypatch.setattr(tasks, "_upload_to_gcs", chamadas)
    task = upload_to_gcs.with_options(retry_delay_seconds=0)

    @flow
    def f():
        return task("x", **ARGS)

    with pytest.raises(type(error)):
        f()
    return chamadas.call_count


def test_schema_mismatch_nao_e_retentado(prefect_harness, monkeypatch):
    assert _run_task_in_flow(monkeypatch, SchemaMismatchError("x")) == 1


def test_outras_falhas_sao_retentadas(prefect_harness, monkeypatch):
    # 1 tentativa + 3 retries
    assert _run_task_in_flow(monkeypatch, RuntimeError("bq instável")) == 4
