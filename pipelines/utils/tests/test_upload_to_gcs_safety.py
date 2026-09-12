"""Garantias de que `_upload_to_gcs` nunca toca a tabela de produção.

Três estragos silenciosos já aconteceram por causa do que estes testes fixam:

1. `dump_mode="overwrite"` chamava `tb.delete(mode="all")`, e `all` percorre
   staging E prod — apagando a tabela materializada de produção, inclusive a
   partir da iteração dev do laço de ambientes.
2. A tabela externa de staging guardava o bucket usado na criação. Como o ramo
   `append` só cria a tabela quando ela não existe, um bucket errado gravado uma
   vez ficava gravado para sempre, e o dbt de produção passava a ler blobs de
   dev sem nenhum erro.
3. As duas funções de sync falavam com a staging por um `bigquery.Client()`
   construído à mão, que cai no ADC do pod e leva 403 — enquanto a `bd.Table`
   ao lado, com as credenciais de staging da lib, lia a mesma tabela sem
   problema.
"""

from unittest.mock import MagicMock, patch

from pipelines.utils.tasks import (
    _staging_client,
    _sync_staging_source_uris,
    _upload_to_gcs,
)

STAGING = "basedosdados-staging.us_sec_edgar_staging.dicionario"


def _bd_table(uris=None):
    """`bd.Table` de mentira cujo cliente de staging devolve `uris`."""
    tb = MagicMock()
    tb.table_full_name = {"staging": STAGING}
    table = MagicMock()
    table.external_data_configuration.source_uris = uris
    tb.client = {"bigquery_staging": MagicMock()}
    tb.client["bigquery_staging"].get_table.return_value = table
    return tb


def test_staging_client_is_the_libs_not_a_fresh_one():
    """O ponto do 403: o cliente tem de vir da `bd.Table`, não do ADC."""
    tb = _bd_table()
    assert _staging_client(tb) is tb.client["bigquery_staging"]


def test_repoints_when_only_the_bucket_differs():
    tb = _bd_table(["gs://basedosdados-dev/staging/us_sec_edgar/dicionario/*"])

    _sync_staging_source_uris(
        tb=tb,
        bucket_name="basedosdados",
        dataset_id="us_sec_edgar",
        table_id="dicionario",
    )

    client = tb.client["bigquery_staging"]
    client.update_table.assert_called_once()
    updated, fields = client.update_table.call_args[0]
    assert fields == ["external_data_configuration"]
    assert updated.external_data_configuration.source_uris == [
        "gs://basedosdados/staging/us_sec_edgar/dicionario/*"
    ]


def test_noop_when_already_correct():
    tb = _bd_table(["gs://basedosdados/staging/us_sec_edgar/dicionario/*"])

    _sync_staging_source_uris(
        tb=tb,
        bucket_name="basedosdados",
        dataset_id="us_sec_edgar",
        table_id="dicionario",
    )

    tb.client["bigquery_staging"].update_table.assert_not_called()


def test_leaves_non_conventional_uris_alone():
    """Várias URIs, ou caminho fora da convenção: avisa e não mexe."""
    tb = _bd_table(
        [
            "gs://outro/caminho/custom/*",
            "gs://outro/caminho/extra/*",
        ]
    )

    _sync_staging_source_uris(
        tb=tb,
        bucket_name="basedosdados",
        dataset_id="us_sec_edgar",
        table_id="dicionario",
    )

    tb.client["bigquery_staging"].update_table.assert_not_called()


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
