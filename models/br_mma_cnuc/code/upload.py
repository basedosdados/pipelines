import basedosdados as bd

DATASET_ID = "br_mma_cnuc"
TABLE_ID = "unidades_conservacao"
BILLING_PROJECT = "basedosdados-dev"

tb = bd.Table(dataset_id=DATASET_ID, table_id=TABLE_ID)

path_to_data = "/Users/rdahis/Downloads/CNUC/output/unidades_conservacao"

tb.create(
    path=path_to_data,
    if_storage_data_exists="replace",
    if_table_exists="replace",
    source_format="parquet",
)
