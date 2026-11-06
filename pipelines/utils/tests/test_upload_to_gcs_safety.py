"""Garantia de que `_upload_to_gcs` nunca apaga a tabela de produção.

`dump_mode="overwrite"` chamava `tb.delete(mode="all")`, e `all` percorre
staging E prod, apagando a tabela materializada de produção. Como o `bd.Table`
monta os nomes das tabelas pelo config do pod, isso acontecia até na etapa de
dev quando o flow rodava no pool de prod (ver #2296).
"""

from unittest.mock import patch

from pipelines.utils.tasks import _upload_to_gcs

STAGING = "basedosdados-staging.us_sec_edgar_staging.dicionario"


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
