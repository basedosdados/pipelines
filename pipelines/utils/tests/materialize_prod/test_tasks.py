"""
Testes de `pipelines/utils/materialize_prod/tasks.py::copy_staging_to_prod`
e `sync_prod_staging_table`.

`copy_staging_to_prod` substitui o download+upload antigo por cópia
server-side (`Bucket.copy_blob`) — ver
`ftwca/investigacoes/pipelines/promocao-dev-prod-pipelines-vs-table-approve-2026-11-03.md`
pro desenho completo. Os testes usam dublês mínimos de
`google.cloud.storage` (sem bater em GCS de verdade).
"""

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipelines.utils.materialize_prod.tasks import (
    copy_staging_to_prod,
    sync_prod_staging_table,
)


class _FakeBlob:
    def __init__(self, name: str, bucket: "_FakeBucket"):
        self.name = name
        self.bucket = bucket
        self.deleted = False

    def delete(self) -> None:
        self.deleted = True
        self.bucket.blobs.pop(self.name, None)

    def download_to_filename(self, filename: str) -> None:
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_text(f"conteúdo de {self.name}")


class _FakeBucket:
    def __init__(self, name: str):
        self.name = name
        self.blobs: dict[str, _FakeBlob] = {}

    def add_blob(self, name: str) -> _FakeBlob:
        blob = _FakeBlob(name, self)
        self.blobs[name] = blob
        return blob

    def list_blobs(self, prefix: str) -> list[_FakeBlob]:
        return [b for name, b in self.blobs.items() if name.startswith(prefix)]

    def copy_blob(
        self, blob: _FakeBlob, destination_bucket: "_FakeBucket", new_name: str
    ) -> _FakeBlob:
        return destination_bucket.add_blob(new_name)


class _FakeClient:
    def __init__(self, buckets: dict[str, _FakeBucket]):
        self._buckets = buckets

    def bucket(self, bucket_name: str, user_project: str | None = None):
        return self._buckets[bucket_name]


@contextmanager
def _patched_client(buckets: dict[str, _FakeBucket]):
    """Mocka `storage.Client` e as credenciais de prod juntos.

    Args:
        buckets: buckets falsos, por nome, a usar no lugar dos reais.
    """
    with (
        patch(
            "pipelines.utils.materialize_prod.tasks.storage.Client",
            return_value=_FakeClient(buckets),
        ),
        patch(
            "pipelines.utils.materialize_prod.tasks._prod_storage_credentials",
            return_value=None,
        ),
    ):
        yield


def _buckets_with(dev_blobs: list[str], prod_blobs: list[str] | None = None):
    dev = _FakeBucket("basedosdados-dev")
    prod = _FakeBucket("basedosdados")
    backup = _FakeBucket("basedosdados-backup")
    for name in dev_blobs:
        dev.add_blob(name)
    for name in prod_blobs or []:
        prod.add_blob(name)
    return {
        "basedosdados-dev": dev,
        "basedosdados": prod,
        "basedosdados-backup": backup,
    }


def test_copy_staging_to_prod_sem_folders_copia_tudo():
    buckets = _buckets_with(
        dev_blobs=["staging/ds/tb/arquivo1.csv", "staging/ds/tb/arquivo2.csv"]
    )

    with _patched_client(buckets):
        sample_path = copy_staging_to_prod.fn(dataset_id="ds", table_id="tb")

    prod_names = set(buckets["basedosdados"].blobs)
    assert prod_names == {
        "staging/ds/tb/arquivo1.csv",
        "staging/ds/tb/arquivo2.csv",
    }
    assert Path(sample_path).exists()


def test_copy_staging_to_prod_sem_folders_faz_backup_e_apaga_prod_antigo():
    buckets = _buckets_with(
        dev_blobs=["staging/ds/tb/novo.csv"],
        prod_blobs=["staging/ds/tb/antigo.csv"],
    )

    with _patched_client(buckets):
        copy_staging_to_prod.fn(dataset_id="ds", table_id="tb")

    assert "staging/ds/tb/antigo.csv" in buckets["basedosdados-backup"].blobs
    prod_names = set(buckets["basedosdados"].blobs)
    assert prod_names == {"staging/ds/tb/novo.csv"}


def test_copy_staging_to_prod_com_folders_nao_mexe_no_resto_do_prod():
    buckets = _buckets_with(
        dev_blobs=[
            "staging/ds/tb/ano=2025/arquivo.csv",
            "staging/ds/tb/ano=2026/arquivo.csv",
        ],
        prod_blobs=["staging/ds/tb/ano=2025/arquivo.csv"],
    )

    with _patched_client(buckets):
        copy_staging_to_prod.fn(
            dataset_id="ds", table_id="tb", folders=["ano=2026"]
        )

    # Partição antiga intocada, backup nunca acionado.
    assert (
        "staging/ds/tb/ano=2025/arquivo.csv" in buckets["basedosdados"].blobs
    )
    assert (
        "staging/ds/tb/ano=2026/arquivo.csv" in buckets["basedosdados"].blobs
    )
    assert not buckets["basedosdados-backup"].blobs


def test_copy_staging_to_prod_com_folders_sem_dado_novo_levanta_erro():
    buckets = _buckets_with(dev_blobs=["staging/ds/tb/ano=2025/arquivo.csv"])

    with (
        _patched_client(buckets),
        pytest.raises(FileNotFoundError),
    ):
        copy_staging_to_prod.fn(
            dataset_id="ds", table_id="tb", folders=["ano=2026"]
        )


def test_copy_staging_to_prod_amostra_preserva_pasta_de_particao():
    buckets = _buckets_with(
        dev_blobs=["staging/ds/tb/ano=2026/mes=11/arquivo.csv"]
    )

    with _patched_client(buckets):
        sample_path = copy_staging_to_prod.fn(dataset_id="ds", table_id="tb")

    sample_files = list(Path(sample_path).rglob("arquivo.csv"))
    assert len(sample_files) == 1
    assert "ano=2026" in str(sample_files[0])
    assert "mes=11" in str(sample_files[0])


def test_sync_prod_staging_table_recreate_chama_table_create_e_limpa_header():
    fake_tb = MagicMock()
    fake_tb.table_exists.return_value = False

    with (
        patch(
            "pipelines.utils.materialize_prod.tasks.bd.Table",
            return_value=fake_tb,
        ),
        patch(
            "pipelines.utils.materialize_prod.tasks.dump_header",
            return_value=("/tmp/fake-header", "parquet"),
        ) as mock_dump_header,
        patch(
            "pipelines.utils.materialize_prod.tasks._delete_header_blobs"
        ) as mock_delete_header,
        patch(
            "pipelines.utils.materialize_prod.tasks._sync_staging_schema"
        ) as mock_sync_schema,
        patch(
            "pipelines.utils.materialize_prod.tasks.bd.Dataset"
        ) as mock_dataset,
    ):
        sync_prod_staging_table.fn(
            dataset_id="ds",
            table_id="tb",
            sample_path="/tmp/sample",
            source_format="csv",
            recreate=True,
        )

    mock_dump_header.assert_called_once_with(
        data_path="/tmp/sample", source_format="csv"
    )
    fake_tb.create.assert_called_once()
    assert fake_tb.create.call_args.kwargs["source_format"] == "parquet"
    mock_delete_header.assert_called_once()
    mock_sync_schema.assert_not_called()
    mock_dataset.assert_called_once_with("ds")
    mock_dataset.return_value.update.assert_called_once_with(mode="prod")


def test_sync_prod_staging_table_sem_recreate_so_sincroniza_schema():
    fake_tb = MagicMock()
    fake_tb.table_exists.return_value = True

    with (
        patch(
            "pipelines.utils.materialize_prod.tasks.bd.Table",
            return_value=fake_tb,
        ),
        patch(
            "pipelines.utils.materialize_prod.tasks._sync_staging_schema"
        ) as mock_sync_schema,
        patch(
            "pipelines.utils.materialize_prod.tasks.dump_header",
            return_value=("/tmp/fake-header", "csv"),
        ) as mock_dump_header,
        patch(
            "pipelines.utils.materialize_prod.tasks._delete_header_blobs"
        ) as mock_delete_header,
    ):
        sync_prod_staging_table.fn(
            dataset_id="ds",
            table_id="tb",
            sample_path="/tmp/sample",
            source_format="csv",
            recreate=False,
        )

    mock_dump_header.assert_called_once_with(
        data_path="/tmp/sample", source_format="csv"
    )
    mock_sync_schema.assert_called_once_with(
        tb=fake_tb, data_sample_path="/tmp/fake-header", source_format="csv"
    )
    fake_tb.create.assert_not_called()
    mock_delete_header.assert_not_called()
    mock_delete_header.assert_not_called()


def test_sync_prod_staging_table_sem_recreate_mas_tabela_nao_existe_cria():
    """`folders` informado (recreate=False) mas é a primeira vez que essa
    tabela é promovida — tabela ainda não existe, então cria mesmo assim."""
    fake_tb = MagicMock()
    fake_tb.table_exists.return_value = False

    with (
        patch(
            "pipelines.utils.materialize_prod.tasks.bd.Table",
            return_value=fake_tb,
        ),
        patch(
            "pipelines.utils.materialize_prod.tasks.dump_header",
            return_value=("/tmp/fake-header", "csv"),
        ),
        patch("pipelines.utils.materialize_prod.tasks._delete_header_blobs"),
        patch(
            "pipelines.utils.materialize_prod.tasks._sync_staging_schema"
        ) as mock_sync_schema,
        patch("pipelines.utils.materialize_prod.tasks.bd.Dataset"),
    ):
        sync_prod_staging_table.fn(
            dataset_id="ds",
            table_id="tb",
            sample_path="/tmp/sample",
            source_format="csv",
            recreate=False,
        )

    fake_tb.create.assert_called_once()
    mock_sync_schema.assert_not_called()
