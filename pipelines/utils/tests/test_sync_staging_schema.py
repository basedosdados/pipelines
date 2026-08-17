from unittest.mock import MagicMock, patch

from google.cloud import bigquery

from pipelines.utils.tasks import (
    _model_sql,
    _referenced_in_model,
    _sync_staging_schema,
)

STAGING_TABLE = "projeto.conjunto_staging.tabela"


def _table() -> MagicMock:
    """Tabela `bd.Table` fingida, apontando para a staging."""
    tb = MagicMock()
    tb.dataset_id = "conjunto"
    tb.table_id = "tabela"
    tb.table_full_name = {"staging": STAGING_TABLE}
    return tb


def _bq_client(*column_names: str) -> MagicMock:
    client = MagicMock()
    client.get_table.return_value = MagicMock(
        schema=[bigquery.SchemaField(name, "STRING") for name in column_names]
    )
    return client


def test_model_sql_encontra_as_duas_grafias(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    models = tmp_path / "models" / "conjunto"
    models.mkdir(parents=True)

    (models / "conjunto__tabela.sql").write_text("select 1", encoding="utf-8")
    assert _model_sql("conjunto", "tabela") == "select 1"

    (models / "conjunto__tabela.sql").unlink()
    (models / "tabela.sql").write_text("select 2", encoding="utf-8")
    assert _model_sql("conjunto", "tabela") == "select 2"


def test_model_sql_sem_arquivo(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert _model_sql("conjunto", "tabela") is None


def test_referenced_in_model_casa_palavra_inteira():
    sql = "select safe_cast(num_area as float64) area from staging"

    assert _referenced_in_model("num_area", sql)
    assert _referenced_in_model("NUM_AREA", sql)
    # `area` aparece como apelido, `num_are` é prefixo de outra coluna
    assert _referenced_in_model("area", sql)
    assert not _referenced_in_model("num_are", sql)
    assert not _referenced_in_model("dat_criaca", sql)


@patch("pipelines.utils.tasks.bigquery.Client")
@patch("pipelines.utils.tasks.dump_header", return_value="header/")
def test_coluna_que_o_modelo_nao_le_nao_altera_a_tabela(
    _mock_dump_header, mock_client_cls, tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    models = tmp_path / "models" / "conjunto"
    models.mkdir(parents=True)
    (models / "conjunto__tabela.sql").write_text(
        "select safe_cast(cod_imovel as string) id_imovel from staging",
        encoding="utf-8",
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
    )

    client.update_table.assert_not_called()
    assert "dat_criaca" in capsys.readouterr().out


@patch("pipelines.utils.tasks.bigquery.Client")
@patch("pipelines.utils.tasks.dump_header", return_value="header/")
def test_coluna_onboardada_no_modelo_e_acrescentada(
    _mock_dump_header, mock_client_cls, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    models = tmp_path / "models" / "conjunto"
    models.mkdir(parents=True)
    (models / "conjunto__tabela.sql").write_text(
        "select safe_cast(indicador_renegociacao as string) indicador_renegociacao",
        encoding="utf-8",
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
    _mock_dump_header, mock_client_cls, tmp_path, monkeypatch
):
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
    )

    client.update_table.assert_called_once()
