"""
Envia particoes incrementais para o staging no bucket dev.

Destino: gs://basedosdados-dev/staging/br_inpe_queimadas/microdados/

Uso (apos processing.py):
    bash preparar_upload_append.sh
    uv run python upload_append.py
"""

from pathlib import Path

import basedosdados as bd

DATASET_ID = "br_inpe_queimadas"
TABLE_ID = "microdados"
BUCKET_NAME = "basedosdados-dev"
UPLOAD_PATH = Path("./output/upload_append")


def main() -> None:
    if not UPLOAD_PATH.exists():
        raise FileNotFoundError(
            f"Pasta nao encontrada: {UPLOAD_PATH}. Rode: bash preparar_upload_append.sh"
        )

    storage = bd.Storage(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name=BUCKET_NAME,
    )

    print(f"Dataset: {DATASET_ID}")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Origem: {UPLOAD_PATH}")
    print(f"Destino: staging/{DATASET_ID}/{TABLE_ID}/")

    storage.upload(
        path=str(UPLOAD_PATH),
        mode="staging",
        if_exists="replace",
    )

    print("Upload para o bucket concluido.")


if __name__ == "__main__":
    main()
