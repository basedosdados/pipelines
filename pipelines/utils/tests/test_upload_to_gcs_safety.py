"""Garantia de que `_upload_to_gcs` nunca apaga a tabela de produção.

`dump_mode="overwrite"` chamava `tb.delete(mode="all")`, e `all` percorre
staging E prod, apagando a tabela materializada de produção. Como o `bd.Table`
monta os nomes das tabelas pelo config do pod, isso acontecia até na etapa de
dev quando o flow rodava no pool de prod (ver #2296).
"""

from unittest.mock import MagicMock, patch

import pytest

from pipelines.utils.tasks import _upload_to_gcs

STAGING = "basedosdados-staging.us_sec_edgar_staging.dicionario"


@pytest.mark.parametrize("dump_mode", ["append", "overwrite"])
@patch("pipelines.utils.tasks._sync_staging_schema")
@patch("pipelines.utils.tasks.dump_header")
@patch("pipelines.utils.tasks.bd")
def test_upload_never_deletes_prod(
    bd_mod: MagicMock,
    dump_header_mock: MagicMock,
    _sync_mock: MagicMock,
    dump_mode: str,
) -> None:
    """Com a staging já existente, nenhum `dump_mode` apaga a tabela de prod.

    Args:
        bd_mod: mock do módulo `basedosdados`.
        dump_header_mock: mock de `dump_header`.
        _sync_mock: mock de `_sync_staging_schema` (ramo `append`).
        dump_mode: modo de escrita testado.
    """
    dump_header_mock.return_value = ("header", "parquet")
    tb = bd_mod.Table.return_value
    tb.table_full_name = {"staging": STAGING}
    tb.table_exists.return_value = True

    _upload_to_gcs(
        data_path="data",
        dataset_id="us_sec_edgar",
        table_id="dicionario",
        bucket_name="basedosdados-dev",
        dump_mode=dump_mode,
    )

    for call in tb.delete.call_args_list:
        assert call.kwargs.get("mode") == "staging"
    if dump_mode == "overwrite":
        tb.delete.assert_called_once_with(mode="staging")
    else:
        tb.delete.assert_not_called()
