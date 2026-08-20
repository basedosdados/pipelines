"""Garantias de que `_upload_to_gcs` nunca toca a tabela de produção.

Dois estragos silenciosos já aconteceram por causa do que estes testes fixam:

1. `dump_mode="overwrite"` chamava `tb.delete(mode="all")`, e `all` percorre
   staging E prod — apagando a tabela materializada de produção, inclusive a
   partir da iteração dev do laço de ambientes.
2. A tabela externa de staging guardava o bucket usado na criação. Como o ramo
   `append` só cria a tabela quando ela não existe, um bucket errado gravado uma
   vez ficava gravado para sempre, e o dbt de produção passava a ler blobs de
   dev sem nenhum erro.
"""

from unittest.mock import MagicMock, patch

from pipelines.utils.tasks import _sync_staging_source_uris, _upload_to_gcs

STAGING = "basedosdados-staging.us_sec_edgar_staging.dicionario"


def _table(uris):
    table = MagicMock()
    table.external_data_configuration.source_uris = uris
    return table


def _bd_table():
    tb = MagicMock()
    tb.table_full_name = {"staging": STAGING}
    return tb


@patch("pipelines.utils.tasks.bigquery.Client")
def test_repoints_when_only_the_bucket_differs(client_cls):
    client = client_cls.return_value
    client.get_table.return_value = _table(
        ["gs://basedosdados-dev/staging/us_sec_edgar/dicionario/*"]
    )

    _sync_staging_source_uris(
        tb=_bd_table(),
        bucket_name="basedosdados",
        dataset_id="us_sec_edgar",
        table_id="dicionario",
        billing_project_id="basedosdados",
    )

    client.update_table.assert_called_once()
    updated, fields = client.update_table.call_args[0]
    assert fields == ["external_data_configuration"]
    assert updated.external_data_configuration.source_uris == [
        "gs://basedosdados/staging/us_sec_edgar/dicionario/*"
    ]


@patch("pipelines.utils.tasks.bigquery.Client")
def test_noop_when_already_correct(client_cls):
    client = client_cls.return_value
    client.get_table.return_value = _table(
        ["gs://basedosdados/staging/us_sec_edgar/dicionario/*"]
    )

    _sync_staging_source_uris(
        tb=_bd_table(),
        bucket_name="basedosdados",
        dataset_id="us_sec_edgar",
        table_id="dicionario",
        billing_project_id="basedosdados",
    )

    client.update_table.assert_not_called()


@patch("pipelines.utils.tasks.bigquery.Client")
def test_leaves_non_conventional_uris_alone(client_cls):
    """Várias URIs, ou caminho fora da convenção: avisa e não mexe."""
    client = client_cls.return_value
    client.get_table.return_value = _table(
        [
            "gs://outro/caminho/custom/*",
            "gs://outro/caminho/extra/*",
        ]
    )

    _sync_staging_source_uris(
        tb=_bd_table(),
        bucket_name="basedosdados",
        dataset_id="us_sec_edgar",
        table_id="dicionario",
        billing_project_id="basedosdados",
    )

    client.update_table.assert_not_called()


@patch("pipelines.utils.tasks.dump_header")
@patch("pipelines.utils.tasks.bd")
def test_overwrite_never_deletes_prod(bd_mod, dump_header_mock):
    """O ponto central: `overwrite` só pode apagar staging."""
    dump_header_mock.return_value = "/tmp/header.parquet"
    tb = bd_mod.Table.return_value
    tb.table_full_name = {"staging": STAGING}
    tb.table_exists.return_value = True

    _upload_to_gcs(
        data_path="/tmp/data",
        dataset_id="us_sec_edgar",
        table_id="dicionario",
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
        source_format="parquet",
    )

    tb.delete.assert_called_once_with(mode="staging")
    for call in tb.delete.call_args_list:
        assert call.kwargs.get("mode") != "all"
