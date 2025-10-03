from pathlib import Path

import basedosdados as bd

# Configuração dos diretórios
ROOT = Path("/home/laribrito/BD/pipelines/models/world_iea_timss")
INPUT_DIR = ROOT / "input"
DATASET_ID = "world_iea_timss"  # Nome do dataset no BigQuery

# Lista de arquivos a serem processados
files_to_upload = [
    # school_context_grade_4.csv",
    # "school_context_grade_8.csv",
    # "student_achievement_grade_4.csv",
    # "student_achievement_grade_8.csv",
    # "student_context_grade_4.csv",
    # "student_context_grade_8.csv",
    # "teacher_context_grade_4.csv",
    # "teacher_mathematics_grade_8.csv",
    # "teacher_science_grade_8.csv",
    "dictionary.csv"
]


def upload_file_to_bq(dataset_id, table_id, file_path):
    """Função para upload de um arquivo para o BigQuery"""
    try:
        tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

        tb.create(
            path=file_path,
            if_storage_data_exists="replace",
            if_table_exists="replace",
            source_format="csv",
        )
        print(
            f"Arquivo {file_path} enviado com sucesso como {dataset_id}.{table_id}"
        )
    except Exception as e:
        print(f"Erro ao enviar {file_path}: {e!s}")


# Processar cada arquivo
for file_name in files_to_upload:
    file_path = INPUT_DIR / file_name
    if file_path.exists():
        table_id = file_name.replace(".csv", "")
        upload_file_to_bq(DATASET_ID, table_id, file_path)
    else:
        print(f"Arquivo não encontrado: {file_path}")

print("Processo de upload concluído.")
