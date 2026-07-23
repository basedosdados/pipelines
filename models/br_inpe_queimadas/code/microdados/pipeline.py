import shutil
from pathlib import Path

import basedosdados as bd
from extraction import download
from processing import run_processing

# Identificadores da tabela no BigQuery
DATASET_ID = "br_inpe_queimadas"
TABLE_ID = "microdados"

# Caminhos base (relativos ao diretório deste script)
INPUT_DIR = "./input"
OUTPUT_DIR = "./output/fire_data"
UPLOAD_DIR = "./output/upload_append"
MAP_CITIES_PATH = "./extra/auxiliary_files/map_cities.csv"


# Copia apenas as partições dos anos solicitados para o diretório de upload
# Substitui a lógica do preparar_upload_append.sh
def prepare_upload(year_range: list[int]) -> None:
    upload_path = Path(UPLOAD_DIR)

    # Remove o diretório anterior para evitar partições desatualizadas
    if upload_path.exists():
        shutil.rmtree(upload_path)

    for year in year_range:
        year_dir = Path(OUTPUT_DIR) / f"ano={year}"
        if not year_dir.exists():
            print(f"  aviso: partição ano={year} não encontrada, pulando.")
            continue
        shutil.copytree(year_dir, upload_path / f"ano={year}")
        print(f"  ok: ano={year}")


# Faz upload das partições novas para o GCS via basedosdados
def upload() -> None:
    table = bd.Table(table_id=TABLE_ID, dataset_id=DATASET_ID)
    table.append(filepath=UPLOAD_DIR, if_exists="replace")


# Facade: ponto único de execução do pipeline completo
# Para atualizar, ajuste year_range
def run(year_range: list[int]) -> None:
    print(f"Iniciando pipeline | anos: {year_range}")

    print("1/4 Download...")
    download(year_range, INPUT_DIR)

    print("2/4 Processamento...")
    run_processing(
        input_path=f"{INPUT_DIR}/month_fire_data_new.csv",
        output_dir=OUTPUT_DIR,
        map_cities_path=MAP_CITIES_PATH,
    )

    print("3/4 Preparando partições para upload...")
    prepare_upload(year_range)

    print("4/4 Upload para storage...")
    upload()

    print(f"Pipeline concluído. Anos enviados: {year_range}")


if __name__ == "__main__":
    # Para atualizar: ajuste year_range
    run(year_range=[2025, 2026])
