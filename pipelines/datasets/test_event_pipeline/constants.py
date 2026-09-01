"""
Constant values for the test_event_pipeline pipeline
"""

# Convenção de resource id / tags do Prefect (automações do piloto) — não é
# o dataset_id real do backend/BigQuery, ver BACKEND_* abaixo.
DATASET_ID = "test_event_pipeline"

# Dataset/tabela reais no backend + BigQuery, criados pra este piloto
# (basedosdados-dev.test_dataset.test_event_pipeline, cadastro em prod).
BACKEND_DATASET_ID = "test_dataset"
BACKEND_TABLE_ID = "test_event_pipeline"
BACKEND_ENV = "prod"
