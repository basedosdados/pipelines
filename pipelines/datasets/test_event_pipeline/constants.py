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

# Recursos de pod pras etapas deste dataset (issue #1867) — sobrescrevem o
# default do work pool (2 CPU / 4Gi de limite; 500m CPU / 1Gi de request).
# Cada dataset declara os seus, já que o tamanho real do download varia
# muito entre datasets (mesma convenção já usada hoje pra flows pesados
# como br_anatel_telefonia_movel — ver
# tutoriais/iac/prefect3-override-de-recursos-por-flow.md no ftwca).
# Este dataset é sintético e leve, então os tiers abaixo são conservadores
# de propósito (checar apenas uma fonte, baixar um CSV pequeno).
#
# `memory_request` do check_update precisa ser explicitamente menor que o
# default do pool (1Gi) porque o Kubernetes rejeita um pod cujo request >
# limit — sem isso, ficaria maior que o `memory_limit` (512Mi) e o deploy
# falharia.
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
