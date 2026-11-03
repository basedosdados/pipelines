#!/usr/bin/env python3
"""Baixa o staging de br_bd_diretorios_brasil/municipio do bucket basedosdados-dev.
Corrige e sobe novamente"""

import argparse
import sys
from pathlib import Path

import google.auth
import pandas as pd
from google.cloud import storage

from pipelines.utils.tasks import _upload_to_gcs

BUCKET = "basedosdados-dev"
PREFIXO = "staging/br_bd_diretorios_brasil/municipio/"
TMP = Path("tmp/br_bd_diretorios_brasil")
INPUT = TMP / "input"
OUTPUT = TMP / "output"
CREDENTIALS_PATH = Path("/home/luizavbo/.basedosdados/credentials/prod.json")
BILLING_PROJECT = None


def cliente() -> tuple[storage.Client, str]:
    credenciais, projeto_arquivo = google.auth.load_credentials_from_file(
        str(CREDENTIALS_PATH)
    )
    projeto = BILLING_PROJECT or projeto_arquivo
    if not projeto:
        sys.exit(
            "defina BILLING_PROJECT: o bucket é requester pays e o arquivo de credenciais não tem project_id"
        )
    return storage.Client(project=projeto, credentials=credenciais), projeto


def baixa(destino: Path = INPUT) -> list[Path]:
    destino.mkdir(parents=True, exist_ok=True)
    client, projeto = cliente()
    bucket = client.bucket(BUCKET, user_project=projeto)
    blobs = [
        b
        for b in bucket.list_blobs(prefix=PREFIXO)
        if not b.name.endswith("/")
    ]
    if not blobs:
        sys.exit(f"nenhum objeto em gs://{BUCKET}/{PREFIXO}")
    arquivos = []
    for blob in blobs:
        local = destino / Path(blob.name).relative_to(PREFIXO.rstrip("/"))
        local.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(local)
        print(
            f"baixado gs://{BUCKET}/{blob.name} ({blob.size} bytes) -> {local}"
        )
        arquivos.append(local)
    return arquivos


def corrige(
    arquivo: Path, valor: str, id_municipio: str, output: Path = OUTPUT
) -> None:
    df = pd.read_csv(arquivo)
    output.mkdir(parents=True, exist_ok=True)
    filename = arquivo.stem
    print(
        f"{len(df[(df['id_municipio'] == id_municipio) & (df['id_municipio_rf'].isna())])} valor a ser alterado"
    )
    df.loc[
        (df["id_municipio"] == id_municipio) & (df["id_municipio_rf"].isna()),
        "id_municipio_rf",
    ] = valor
    df.to_csv(output / filename, index=False)
    print("Arquivo salvo")
    _upload_to_gcs(
        output / filename, "br_bd_diretorios_brasil", "municipio", BUCKET
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--corrige",
        metavar="VALOR",
        help="valor para id_municipio_rf vazio (ex.: 1182)",
    )
    ap.add_argument("--id_municipio")
    args = ap.parse_args()

    arquivos = baixa()
    if args.corrige is not None:
        for a in arquivos:
            if a.suffix.lower() == ".csv":
                corrige(a, args.corrige, args.id_municipio)


if __name__ == "__main__":
    main()
