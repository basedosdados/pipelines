from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from pipelines.utils.tasks import (
    _model_sql,
    _referenced_in_model,
    _sync_staging_schema,
)

STAGING_TABLE = "projeto.conjunto_staging.tabela"


def _table() -> MagicMock:
    """Tabela `bd.Table` fingida, apontando para a staging.

    Returns:
        Um mock com `dataset_id`, `table_id` e `table_full_name` preenchidos.
    """
    tb = MagicMock()
    tb.dataset_id = "conjunto"
    tb.table_id = "tabela"
    tb.table_full_name = {"staging": STAGING_TABLE}
    return tb


def _bq_client(*column_names: str) -> MagicMock:
    """Cliente do BigQuery fingido, cuja tabela tem as colunas informadas.

    Args:
        *column_names: nomes das colunas já presentes no schema da staging.

    Returns:
        Um mock cujo `get_table` devolve uma tabela com esse schema.
    """
    client = MagicMock()
    client.get_table.return_value = MagicMock(
        schema=[bigquery.SchemaField(name, "STRING") for name in column_names]
    )
    return client


def _write_model(tmp_path: Path, name: str, sql: str) -> None:
    """Escreve um `.sql` em `models/conjunto/` dentro do diretório temporário.

    Args:
        tmp_path: raiz temporária que faz as vezes do repositório.
        name: nome do arquivo, com extensão.
        sql: conteúdo do modelo.
    """
    models = tmp_path / "models" / "conjunto"
    models.mkdir(parents=True, exist_ok=True)
    (models / name).write_text(sql, encoding="utf-8")


def test_model_sql_segue_o_dbt_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Escolhe o arquivo pela mesma regra que `run_dbt`, sem cair no outro."""
    monkeypatch.chdir(tmp_path)
    _write_model(tmp_path, "conjunto__tabela.sql", "select 1")
    _write_model(tmp_path, "tabela.sql", "select 2")

    assert _model_sql("conjunto", "tabela", dbt_alias=True) == "select 1"
    assert _model_sql("conjunto", "tabela", dbt_alias=False) == "select 2"


def test_model_sql_nao_usa_a_outra_grafia_como_alternativa(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Só existe o arquivo com prefixo, mas o flow roda sem alias: nada é lido."""
    monkeypatch.chdir(tmp_path)
    _write_model(tmp_path, "conjunto__tabela.sql", "select 1")

    assert _model_sql("conjunto", "tabela", dbt_alias=False) is None


def test_model_sql_sem_arquivo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Conjunto cujo modelo vive fora da convenção devolve `None`."""
    monkeypatch.chdir(tmp_path)

    assert _model_sql("conjunto", "tabela", dbt_alias=True) is None


def test_referenced_in_model_casa_palavra_inteira() -> None:
    """Casa nome inteiro, ignora caixa e não casa prefixo de outra coluna."""
    sql = "select safe_cast(num_area as float64) area from staging"

    assert _referenced_in_model("num_area", sql)
    assert _referenced_in_model("NUM_AREA", sql)
    assert _referenced_in_model("area", sql)
    assert not _referenced_in_model("num_are", sql)
    assert not _referenced_in_model("dat_criaca", sql)


@patch("pipelines.utils.tasks.bigquery.Client")
@patch("pipelines.utils.tasks.dump_header", return_value="header/")
def test_coluna_que_o_modelo_nao_le_nao_altera_a_tabela(
    _mock_dump_header: MagicMock,
    mock_client_cls: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Coluna nova ausente do modelo é ignorada, e o schema não é alterado."""
    monkeypatch.chdir(tmp_path)
    _write_model(
        tmp_path,
        "conjunto__tabela.sql",
        "select safe_cast(cod_imovel as string) id_imovel from staging",
    )

    tb = _table()
    tb._load_staging_schema_from_data.return_value = [
        bigquery.SchemaField("cod_imovel", "STRING"),
        bigquery.SchemaField("dat_criaca", "STRING"),
    ]
    client = _bq_client("cod_imovel")
    mock_client_cls.return_value = client

    _sync_staging_schema(
        tb=tb,
        data_path="dados/",
        source_format="parquet",
        billing_project_id="projeto",
        dbt_alias=True,
    )

    client.update_table.assert_not_called()
    assert "dat_criaca" in capsys.readouterr().out


@patch("pipelines.utils.tasks.bigquery.Client")
@patch("pipelines.utils.tasks.dump_header", return_value="header/")
def test_coluna_onboardada_no_modelo_e_acrescentada(
    _mock_dump_header: MagicMock,
    mock_client_cls: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coluna citada no modelo entra no fim do schema, sem mexer nas demais."""
    monkeypatch.chdir(tmp_path)
    _write_model(
        tmp_path,
        "conjunto__tabela.sql",
        "select safe_cast(indicador_renegociacao as string) indicador_renegociacao",
    )

    tb = _table()
    tb._load_staging_schema_from_data.return_value = [
        bigquery.SchemaField("indicador_renegociacao", "STRING"),
    ]
    client = _bq_client("outra_coluna")
    mock_client_cls.return_value = client

    _sync_staging_schema(
        tb=tb,
        data_path="dados/",
        source_format="parquet",
        billing_project_id="projeto",
        dbt_alias=True,
    )

    client.update_table.assert_called_once()
    table, fields = client.update_table.call_args[0]
    assert fields == ["schema"]
    assert [field.name for field in table.schema] == [
        "outra_coluna",
        "indicador_renegociacao",
    ]


@patch("pipelines.utils.tasks.bigquery.Client")
@patch("pipelines.utils.tasks.dump_header", return_value="header/")
def test_sem_arquivo_de_modelo_sincroniza_tudo(
    _mock_dump_header: MagicMock,
    mock_client_cls: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sem `.sql` para consultar, o filtro não se aplica e tudo é sincronizado."""
    monkeypatch.chdir(tmp_path)

    tb = _table()
    tb._load_staging_schema_from_data.return_value = [
        bigquery.SchemaField("coluna_nova", "STRING"),
    ]
    client = _bq_client("coluna_antiga")
    mock_client_cls.return_value = client

    _sync_staging_schema(
        tb=tb,
        data_path="dados/",
        source_format="parquet",
        billing_project_id="projeto",
        dbt_alias=True,
    )

    client.update_table.assert_called_once()
