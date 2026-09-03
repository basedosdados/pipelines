"""
Constant values for the test_event_pipeline_partitioned pipeline
"""

# Dataset/tabela reais no backend + BigQuery, criados pra testar a variante
# particionada (ano=/mes=) da arquitetura orientada a eventos (issue
# basedosdados/pipelines#1867) — mesmo dataset do piloto original
# (test_event_pipeline), tabela nova.
DATASET_ID = "test_dataset"
TABLE_ID = "test_event_pipeline_partitioned"
BACKEND_ENV = "prod"

# Convenção de nome de deployment/tags do Prefect (ver
# pipelines/utils/stage_dispatch.py) — não é o dataset_id real do
# backend/BigQuery acima, é só o nome usado nos deployments/flows deste
# piloto.
PREFECT_DATASET_ID = "test_event_pipeline_partitioned"

# Recursos de pod pras etapas deste dataset (issue #1867) — mesmos tiers
# do piloto original, já que a carga é igualmente sintética/leve (só um
# CSV pequeno a mais por partição). Ver
# tutoriais/iac/prefect3-override-de-recursos-por-flow.md no ftwca.
JOB_VARIABLES = {
    "check_update": {
        "cpu_limit": "500m",
        "memory_limit": "512Mi",
        "memory_request": "256Mi",
    },
    "flow_download": {
        "cpu_limit": "1",
        "memory_limit": "2Gi",
    },
}
