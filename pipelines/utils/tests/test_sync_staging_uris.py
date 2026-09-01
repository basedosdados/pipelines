"""Testes de `_leg_owns_staging_table` e `_sync_staging_uris`.

O projeto BigQuery da staging vem do `config.toml` do pod e é o mesmo nas duas
pernas do flow; só a perna cujo bucket casa com esse projeto pode alterar a
definição da tabela externa.
"""

from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery

from pipelines.utils.tasks import _leg_owns_staging_table, _sync_staging_uris

DATASET = "br_senatran_estatisticas"
TABLE = "municipio_combustivel"


def _fake_table(staging_project: str, bucket_name: str) -> MagicMock:
    """Um `bd.Table` mínimo: só o que as duas funções leem."""
    tb = MagicMock()
    tb.dataset_id = DATASET
    tb.table_id = TABLE
    tb.uri = f"gs://{bucket_name}" + "/staging/{dataset}/{table}/*"
    tb.table_full_name = {
        "staging": f"{staging_project}.{DATASET}_staging.{TABLE}"
    }
    tb.client = {"bigquery_staging": MagicMock(project=staging_project)}
    return tb


def _external_config(bucket_name: str) -> bigquery.ExternalConfig:
    config = bigquery.ExternalConfig("PARQUET")
    prefix = f"gs://{bucket_name}/staging/{DATASET}/{TABLE}/"
    config.source_uris = [prefix + "*"]
    hive = bigquery.external_config.HivePartitioningOptions.from_api_repr(
        {
            "mode": "STRINGS",
            "sourceUriPrefix": prefix,
            "fields": ["ano", "mes"],
        }
    )
    config.hive_partitioning = hive
    return config


# ── quem é dono da definição ──────────────────────────────────────────────


@pytest.mark.parametrize(
    ("staging_project", "bucket_name", "esperado"),
    [
        # worker de dev: só a perna de dev casa
        ("basedosdados-dev", "basedosdados-dev", True),
        ("basedosdados-dev", "basedosdados", False),
        # worker de prod: só a perna de prod casa
        ("basedosdados-staging", "basedosdados", True),
        ("basedosdados-staging", "basedosdados-dev", False),
        # bucket desconhecido nunca é dono
        ("basedosdados-dev", "outro-bucket", False),
    ],
)
def test_leg_owns_staging_table(staging_project, bucket_name, esperado):
    tb = _fake_table(staging_project, bucket_name)
    assert _leg_owns_staging_table(tb, bucket_name) is esperado


# ── o reaponte em si ──────────────────────────────────────────────────────


def test_reaponta_quando_a_tabela_aponta_para_o_bucket_errado():
    """O caso real: perna de prod achando a URI de dev que a perna de dev deixou."""
    tb = _fake_table("basedosdados-staging", "basedosdados")
    client = tb.client["bigquery_staging"]
    table = MagicMock()
    table.external_data_configuration = _external_config("basedosdados-dev")
    client.get_table.return_value = table

    _sync_staging_uris(tb=tb, bucket_name="basedosdados")

    client.update_table.assert_called_once()
    atualizada, campos = client.update_table.call_args[0]
    assert campos == ["external_data_configuration"]

    config = atualizada.external_data_configuration
    prefixo = f"gs://basedosdados/staging/{DATASET}/{TABLE}/"
    assert config.source_uris == [prefixo + "*"]
    assert config.hive_partitioning.source_uri_prefix == prefixo
    # as colunas de partição têm que sobreviver ao reaponte
    assert config.hive_partitioning.to_api_repr()["fields"] == ["ano", "mes"]
    assert config.hive_partitioning.mode == "STRINGS"


def test_nao_faz_nada_quando_ja_esta_certo():
    tb = _fake_table("basedosdados-staging", "basedosdados")
    client = tb.client["bigquery_staging"]
    table = MagicMock()
    table.external_data_configuration = _external_config("basedosdados")
    client.get_table.return_value = table

    _sync_staging_uris(tb=tb, bucket_name="basedosdados")

    client.update_table.assert_not_called()


def test_perna_de_dev_no_worker_de_prod_nao_toca_na_tabela():
    """A regressão que o guard existe para evitar.

    Sem ele, a perna de dev reapontaria a tabela de `basedosdados-staging`
    para o bucket de dev — e um table-approve seguinte materializaria prod a
    partir dele.
    """
    tb = _fake_table("basedosdados-staging", "basedosdados-dev")
    client = tb.client["bigquery_staging"]

    _sync_staging_uris(tb=tb, bucket_name="basedosdados-dev")

    client.get_table.assert_not_called()
    client.update_table.assert_not_called()


def test_tabela_nao_externa_e_ignorada():
    tb = _fake_table("basedosdados-dev", "basedosdados-dev")
    client = tb.client["bigquery_staging"]
    table = MagicMock()
    table.external_data_configuration = None
    client.get_table.return_value = table

    _sync_staging_uris(tb=tb, bucket_name="basedosdados-dev")

    client.update_table.assert_not_called()
