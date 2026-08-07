"""
Helpers de Google Cloud Storage: upload de dados e artefatos dbt.
"""

import base64
import json
import os
from collections.abc import Generator
from os import getenv, walk
from os.path import join
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.oauth2 import service_account

# ──────────────────────────────────────────────────────────────────────────────
# Credenciais GCS via env vars
# ──────────────────────────────────────────────────────────────────────────────


def get_credentials_from_env(
    mode: str = "prod", scopes: list[str] | None = None
) -> service_account.Credentials:
    if mode not in ["prod", "staging"]:
        raise ValueError("mode deve ser 'prod' ou 'staging'")
    env = getenv(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()}", "")
    if not env:
        raise ValueError(
            f"BASEDOSDADOS_CREDENTIALS_{mode.upper()} não definida"
        )
    info = json.loads(base64.b64decode(env))
    cred = service_account.Credentials.from_service_account_info(info)
    if scopes:
        cred = cred.with_scopes(scopes)
    return cred


def list_blobs_with_prefix(
    bucket_name: str, prefix: str, mode: str = "prod"
) -> list[Blob]:
    credentials = get_credentials_from_env(mode=mode)
    return list(
        storage.Client(credentials=credentials).list_blobs(
            bucket_name, prefix=prefix
        )
    )


# ──────────────────────────────────────────────────────────────────────────────
# dump_header — cria arquivo com apenas o cabeçalho para criação inicial de tabela
# ──────────────────────────────────────────────────────────────────────────────


def dump_header(data_path: str | Path, source_format: str) -> str:
    """
    Escreve apenas o cabeçalho de um CSV ou Parquet para um arquivo temporário.
    Necessário para criar a tabela staging no BQ antes do primeiro upload.
    Retorna o caminho do diretório com o arquivo de cabeçalho.
    """
    path = Path(data_path)
    if not path.is_dir():
        path = path.parent

    file: str | None = None
    for subdir, _, filenames in walk(str(path)):
        for fname in filenames:
            if (source_format == "csv" and fname.endswith(".csv")) or (
                source_format == "parquet" and fname.endswith(".parquet")
            ):
                file = join(subdir, fname)
                break
        if file:
            break

    if file is None:
        raise FileNotFoundError(
            f"Nenhum arquivo {source_format} encontrado em {path}"
        )

    save_header_path = f"data/{uuid4()}"
    partition_folders = [folder for folder in file.split("/") if "=" in folder]
    ext = source_format  # csv ou parquet

    if partition_folders:
        partition_path = "/".join(partition_folders)
        save_header_file_path = Path(
            f"{save_header_path}/{partition_path}/header.{ext}"
        )
    else:
        save_header_file_path = Path(f"{save_header_path}/header.{ext}")

    save_header_file_path.parent.mkdir(parents=True, exist_ok=True)

    if source_format == "csv":
        pd.read_csv(file, nrows=1, dtype=str).to_csv(
            save_header_file_path, index=False, encoding="utf-8"
        )
    elif source_format == "parquet":
        pf = pq.ParquetFile(file)
        pf.read_row_group(0).slice(0, 1).to_pandas().astype(str).to_parquet(
            save_header_file_path, index=False
        )

    return save_header_path


# ──────────────────────────────────────────────────────────────────────────────
# DBTArtifactUploader — sobe artefatos dbt (compiled/run) para o GCS
# ──────────────────────────────────────────────────────────────────────────────


class DBTArtifactUploader:
    """
    Sobe os artefatos do dbt (compiled e run) para o GCS após cada execução.

    Só executa dentro de Kubernetes. Fora, use enable_upload=True para forçar.
    Estrutura no GCS: dbt-artifacts/<dataset_id>/<table_id>/<target>/<subfolder>/<arquivo>
    """

    DEFAULT_SOURCE_DIR = "target"
    DEFAULT_SUBFOLDERS = ("compiled", "run")
    DESTINATION_PREFIX = "dbt-artifacts"
    DEFAULT_TARGET = "dev"

    def __init__(
        self,
        dataset_id: str,
        table_id: str | None,
        target: str = DEFAULT_TARGET,
        bucket_name: str = "basedosdados-dev",
        user_project: str = "basedosdados-dev",
        source_dir: str = DEFAULT_SOURCE_DIR,
        subfolders: tuple[str, ...] = DEFAULT_SUBFOLDERS,
        enable_upload: bool = False,
    ) -> None:
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.target = target
        self.bucket_name = bucket_name
        self.user_project = user_project
        self.source_dir = source_dir
        self.subfolders = subfolders
        self.enable_upload = enable_upload
        self._client: storage.Client | None = None
        self._bucket: storage.Bucket | None = None
        self._deleted_prefixes: set[str] = set()

    def run(self) -> None:
        if not self._should_run():
            print(
                "DBTArtifactUploader: ignorado (não em Kubernetes e enable_upload=False)"
            )
            return
        self._init_gcs()
        print("DBTArtifactUploader: iniciando upload de artefatos dbt")
        try:
            prefix = self._table_prefix()
            self._delete_once(prefix)
            for local_path, subfolder in self._list_files():
                self._upload(local_path, subfolder)
        finally:
            self._cleanup()

    def _should_run(self) -> bool:
        return (
            os.path.exists("/var/run/secrets/kubernetes.io")
            or self.enable_upload
        )

    def _init_gcs(self) -> None:
        # target=prod usa SA prod; demais targets usam SA staging — necessário
        # porque o bucket basedosdados-dev é requester-pays e a SA prod não tem
        # serviceusage.services.use no projeto basedosdados-dev.
        mode = "prod" if self.target == "prod" else "staging"
        credentials = get_credentials_from_env(mode=mode)
        self._client = storage.Client(
            project=self.user_project, credentials=credentials
        )
        self._bucket = self._client.bucket(
            bucket_name=self.bucket_name, user_project=self.user_project
        )

    def _table_prefix(self) -> str:
        return f"{self.DESTINATION_PREFIX}/{self.dataset_id}/{self.table_id}/{self.target}/"

    def _blob_path(self, subfolder: str, filename: str) -> str:
        return f"{self.DESTINATION_PREFIX}/{self.dataset_id}/{self.table_id}/{self.target}/{subfolder}/{filename}"

    def _delete_once(self, prefix: str) -> None:
        if prefix in self._deleted_prefixes:
            return
        # pyrefly: ignore [missing-attribute]
        blobs = list(self._client.list_blobs(self._bucket, prefix=prefix))
        if blobs:
            # pyrefly: ignore [missing-attribute]
            self._bucket.delete_blobs(blobs)
            print(
                f"DBTArtifactUploader: removidos {len(blobs)} artefatos antigos em {prefix}"
            )
        self._deleted_prefixes.add(prefix)

    def _list_files(self) -> Generator[tuple[str, str], None, None]:
        for subfolder in self.subfolders:
            full_path = os.path.join(self.source_dir, subfolder)
            if not os.path.exists(full_path):
                continue
            for root, _, files in os.walk(full_path):
                for file in files:
                    yield os.path.join(root, file), subfolder

    def _upload(self, local_path: str, subfolder: str) -> None:
        filename = os.path.basename(local_path)
        # pyrefly: ignore [missing-attribute]
        blob = self._bucket.blob(self._blob_path(subfolder, filename))
        blob.upload_from_filename(local_path)
        print(f"DBTArtifactUploader: → gs://{self.bucket_name}/{blob.name}")

    def _cleanup(self) -> None:
        import shutil

        if os.path.exists(self.source_dir):
            shutil.rmtree(self.source_dir)
            print(f"DBTArtifactUploader: {self.source_dir} removido")
