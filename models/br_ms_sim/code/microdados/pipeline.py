import shutil
from pathlib import Path

import basedosdados as bd

# pyrefly: ignore [missing-import]
from cleaning import MUNICIPIOS_PATH, process

# pyrefly: ignore [missing-import]
from extraction import download

DATASET_ID = "br_ms_sim"
TABLE_ID = "microdados"

INPUT_DIR = "./input"
OUTPUT_DIR = "./output"
UPLOAD_DIR = "./output/upload_append"


def prepare_upload(year_range: list[int]) -> None:
    upload_path = Path(UPLOAD_DIR)

    if upload_path.exists():
        shutil.rmtree(upload_path)

    for year in year_range:
        year_dir = Path(OUTPUT_DIR) / f"ano={year}"
        if not year_dir.exists():
            print(f"  aviso: partição ano={year} não encontrada, pulando.")
            continue
        shutil.copytree(year_dir, upload_path / f"ano={year}")
        print(f"  ok: ano={year}")


def upload() -> None:
    table = bd.Table(table_id=TABLE_ID, dataset_id=DATASET_ID)
    table.append(filepath=UPLOAD_DIR, if_exists="replace")


def run(year_range: list[int]) -> None:
    print(f"Iniciando pipeline SIM | anos: {year_range}")

    print("1/4 Download...")
    download(year_range, INPUT_DIR)

    print("2/4 Limpeza...")
    process(year_range, INPUT_DIR, OUTPUT_DIR, municipios_path=MUNICIPIOS_PATH)

    print("3/4 Preparando partições para upload...")
    prepare_upload(year_range)

    print("4/4 Upload para staging...")
    upload()

    print(f"Pipeline concluído. Anos enviados: {year_range}")


# Anos no FTP (SIM/CID10/DORES). Conferir antes de rodar:
#   uv run python check_ftp_years.py
# Incluir só anos com 27/27 UFs. 2025+ quando o FTP publicar.
YEAR_RANGE = [2020, 2021, 2022, 2023, 2024]

if __name__ == "__main__":
    run(year_range=YEAR_RANGE)
