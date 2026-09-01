"""
Constant values for the test_event_pipeline pipeline
"""

# Dataset/tabela reais no backend + BigQuery, criados pra este piloto
# (basedosdados-dev.test_dataset.test_event_pipeline, cadastro em prod).
DATASET_ID = "test_dataset"
TABLE_ID = "test_event_pipeline"
BACKEND_ENV = "prod"

# Convenção de nome de deployment/tags do Prefect (ver
# pipelines/utils/stage_dispatch.py) — não é o dataset_id real do
# backend/BigQuery acima, é só o nome usado nos deployments/flows deste
# piloto.
PREFECT_DATASET_ID = "test_event_pipeline"
