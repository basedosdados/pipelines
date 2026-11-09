"""
Tasks for transfer_files_to_prod
"""

import pathlib
from uuid import uuid4

import basedosdados as bd
from basedosdados.core.base import Base
from google.cloud import storage
from google.cloud.storage import Blob, Bucket
from prefect import task

from pipelines.utils.gcs import dump_header
from pipelines.utils.tasks import _sync_staging_schema
from pipelines.utils.utils import log

# ──────────────────────────────────────────────────────────────────────────────
# copy_staging_to_prod — cópia server-side (copy_blob), sem baixar/subir
# pelo worker — ver ftwca/investigacoes/pipelines/
# promocao-dev-prod-pipelines-vs-table-approve-2026-11-03.md
# ──────────────────────────────────────────────────────────────────────────────


def _prod_storage_credentials():
    """Carrega as credenciais de prod do SDK `basedosdados`.

    Returns:
        Credenciais de service account prontas pra passar a `storage.Client`.
    """
    return Base()._load_credentials("prod")


def _delete_blobs_with_prefix(bucket: Bucket, prefix: str) -> int:
    """Apaga todos os blobs sob um prefixo.

    Args:
        bucket: bucket onde apagar.
        prefix: prefixo dos blobs a apagar.

    Returns:
        Quantos blobs foram apagados.
    """
    blobs = list(bucket.list_blobs(prefix=prefix))
    for blob in blobs:
        blob.delete()
    return len(blobs)


def _backup_and_clear_prod(
    destination: Bucket, backup: Bucket, base_prefix: str
) -> list[str]:
    """Faz backup do staging de prod atual e o limpa, pra promoção completa.

    Apaga o backup antigo, copia o staging de prod atual pro backup, e
    depois apaga esse staging de prod.

    Args:
        destination: bucket de prod a limpar.
        backup: bucket onde guardar o backup.
        base_prefix: prefixo base da tabela (`staging/{dataset_id}/{table_id}/`).

    Returns:
        `[base_prefix]` — a tabela inteira é a origem da cópia que vem a
        seguir.
    """
    n_backup_removed = _delete_blobs_with_prefix(backup, base_prefix)
    if n_backup_removed:
        log(f"Backup antigo removido: {n_backup_removed} arquivo(s)")

    existing_prod_blobs = list(destination.list_blobs(prefix=base_prefix))
    if existing_prod_blobs:
        log(
            f"Backup do staging de prod atual: "
            f"{len(existing_prod_blobs)} arquivo(s)"
        )
        for blob in existing_prod_blobs:
            destination.copy_blob(blob, backup, new_name=blob.name)
        _delete_blobs_with_prefix(destination, base_prefix)

    return [base_prefix]


def _copy_blobs_at_prefixes(
    source: Bucket,
    destination: Bucket,
    source_bucket: str,
    prefixes: list[str],
) -> list[Blob]:
    """Copia (server-side) os blobs de `source` sob cada prefixo pra `destination`.

    Args:
        source: bucket de origem.
        destination: bucket de destino.
        source_bucket: nome do bucket de origem — só usado na mensagem de
            erro.
        prefixes: prefixos a copiar.

    Returns:
        Os blobs recém-copiados, já no bucket de destino.

    Raises:
        FileNotFoundError: algum prefixo em `prefixes` não tem blob em
            `source`.
    """
    copied_blobs: list[Blob] = []
    for prefix in prefixes:
        source_blobs = list(source.list_blobs(prefix=prefix))
        if not source_blobs:
            raise FileNotFoundError(
                f"Nenhum arquivo em gs://{source_bucket}/{prefix}"
            )
        for blob in source_blobs:
            copied_blobs.append(
                source.copy_blob(blob, destination, new_name=blob.name)
            )
    return copied_blobs


@task
def copy_staging_to_prod(
    dataset_id: str,
    table_id: str,
    folders: list[str] | None = None,
    source_bucket: str = "basedosdados-dev",
    destination_bucket: str = "basedosdados",
    backup_bucket: str = "basedosdados-backup",
    billing_project: str = "basedosdados",
) -> str:
    """Copia o staging de `dataset_id.table_id` de dev pra prod via cópia
    server-side (`Bucket.copy_blob`) — sem baixar nem subir pelo worker.

    Sem `folders`: comportamento de promoção completa — faz backup do
    staging de prod atual inteiro (pro `backup_bucket`), apaga tudo, e
    copia o staging de dev inteiro. Com `folders`: comportamento de
    promoção parcial — copia só os blobs das subpastas indicadas
    (partições Hive, ex. `["ano=2026/mes=10"]`), sem backup e sem apagar o
    resto do staging de prod.

    Args:
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        folders: pastas de partição estilo Hive a promover. `None` (padrão)
            promove a tabela inteira.
        source_bucket: bucket de origem (requester-pays).
        destination_bucket: bucket de destino.
        backup_bucket: bucket de backup do staging de prod atual — só usado
            quando `folders` é `None`.
        billing_project: projeto cobrado pelo acesso requester-pays e pelas
            operações de cópia.

    Returns:
        Caminho local de um diretório com **um** blob de amostra baixado
        (preserva a subpasta de partição, se houver) — usado só pra
        inferir schema (`dump_header`/`Table.create`), nunca pro dado real.
    """
    client = storage.Client(
        project=billing_project, credentials=_prod_storage_credentials()
    )
    source = client.bucket(source_bucket, user_project=billing_project)
    destination = client.bucket(
        destination_bucket, user_project=billing_project
    )

    base_prefix = f"staging/{dataset_id}/{table_id}/"

    if folders is None:
        backup = client.bucket(backup_bucket, user_project=billing_project)
        source_prefixes = _backup_and_clear_prod(
            destination, backup, base_prefix
        )
    else:
        source_prefixes = [f"{base_prefix}{folder}/" for folder in folders]

    copied_blobs = _copy_blobs_at_prefixes(
        source, destination, source_bucket, source_prefixes
    )

    log(
        f"copy_table: {len(copied_blobs)} arquivo(s) copiados de "
        f"gs://{source_bucket}/{base_prefix} pra gs://{destination_bucket}/{base_prefix}"
    )

    sample_blob = copied_blobs[0]
    assert sample_blob.name is not None
    sample_dir = pathlib.Path(f"/tmp/data/sample/{uuid4()}")
    relative_path = sample_blob.name[len(base_prefix) :]
    local_sample_path = sample_dir / relative_path
    local_sample_path.parent.mkdir(parents=True, exist_ok=True)
    sample_blob.download_to_filename(str(local_sample_path))

    return str(sample_dir)


def _delete_header_blobs(
    bucket_name: str,
    dataset_id: str,
    table_id: str,
    billing_project: str,
) -> None:
    """Apaga só os blobs `header.csv`/`header.parquet` subidos por `Table.create()`.

    Nunca apaga o prefixo inteiro — `Storage.delete_table` apagaria o dado
    real que `copy_staging_to_prod` acabou de copiar.

    Args:
        bucket_name: bucket de prod onde a tabela staging vive.
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        billing_project: projeto cobrado pelas operações no bucket.
    """
    client = storage.Client(
        project=billing_project, credentials=_prod_storage_credentials()
    )
    bucket = client.bucket(bucket_name, user_project=billing_project)
    prefix = f"staging/{dataset_id}/{table_id}/"
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.rsplit("/", 1)[-1] in ("header.csv", "header.parquet"):
            blob.delete()


@task
def sync_prod_staging_table(
    dataset_id: str,
    table_id: str,
    sample_path: str,
    bucket_name: str = "basedosdados",
    recreate: bool = True,
) -> None:
    """Garante que a definição da tabela staging de prod bate com o dado já
    copiado por `copy_staging_to_prod` — nunca faz upload do dado real, só
    lê `sample_path` (um arquivo de amostra local) pra inferir schema.

    Args:
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        sample_path: diretório local com um blob de amostra, devolvido por
            `copy_staging_to_prod`.
        bucket_name: bucket de prod onde a tabela staging vive.
        recreate: `True` (promoção sem `folders`, staging reconstruído do
            zero) recria a definição da tabela via `Table.create`. `False`
            (promoção parcial via `folders`) só acrescenta colunas novas
            ao schema existente — a tabela já deve existir nesse caso.
    """
    billing_project_id = bucket_name
    tb = bd.Table(
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
        billing_project_id=billing_project_id,
    )

    header_path, source_format = dump_header(data_path=sample_path)

    if recreate or not tb.table_exists(mode="staging"):
        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_exists="replace",
            source_format=source_format,
        )
        # `tb.create()` sobe o header.* pro staging como se fosse dado real
        # — o dado de verdade já foi copiado por `copy_staging_to_prod`
        # antes disso. Limpeza cirúrgica, nunca `Storage.delete_table`
        # (apagaria o prefixo inteiro, inclusive o dado real recém-copiado).
        _delete_header_blobs(
            bucket_name=bucket_name,
            dataset_id=dataset_id,
            table_id=table_id,
            billing_project=billing_project_id,
        )
        # Desligado temporariamente: prefect@basedosdados tem WRITER
        # (roles/editor -> projectWriters) mas não OWNER nos datasets do
        # BigQuery, e bd.Dataset.update() exige OWNER (bigquery.datasets.update)
        # pra alterar a descrição. Confirmado via testIamPermissions e teste
        # real (local e no pod) em 2026-11-08 — religar quando a SA tiver
        # essa permissão.
        # bd.Dataset(dataset_id).update(mode="prod")
    else:
        _sync_staging_schema(
            tb=tb, data_sample_path=header_path, source_format=source_format
        )
