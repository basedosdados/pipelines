import basedosdados as bd
from databasers_utils import TableArchitecture

DATASET_ID = "br_mma_sinpatinhas"
TABLE_ID = "patinhas_registradas"

tb = bd.Table(dataset_id=DATASET_ID, table_id=TABLE_ID)

path_to_data = "/models/br_mma_sinpatinhas/output/data.parquet"

tb.create(
    path=path_to_data,
    if_storage_data_exists="raise",
    if_table_exists="replace",
    source_format="parquet",
)

arch = TableArchitecture(
    dataset_id="br_mma_sinpatinhas",
    tables={
        "patinhas_registradas": "https://docs.google.com/spreadsheets/d/1hy-QTdXxmpYbJkyJytjtMwCI00pCcFTrOCXzgFTLRxA/edit?usp=sharing",  # Exemplo https://docs.google.com/spreadsheets/d/1K1svie4Gyqe6NnRjBgJbapU5sTsLqXWTQUmTRVIRwQc/edit?usp=drive_link
    },
)

# Cria o yaml file
arch.create_yaml_file()

# Cria os arquivos sql
arch.create_sql_files()

# Atualiza o dbt_project.yml
arch.update_dbt_project()
