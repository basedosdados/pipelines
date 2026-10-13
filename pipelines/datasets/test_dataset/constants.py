"""
Constant values for test_dataset.

`test_dataset` é uma área de sandbox no backend/BigQuery pra testar
mecanismos de pipeline sem tocar em nenhum produto de dados real. Vários
pilotos convivem aqui — cada um numa seção comentada abaixo, junto do
`tasks.py`/`flows.py` equivalentes (mesma convenção).
"""

from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly

DATASET_ID = "test_dataset"
BACKEND_ENV = "prod"

# Mesma coverage pros dois pilotos (event_pipeline/event_pipeline_partitioned).
EVENT_PIPELINE_COVERAGE = AllFree(
    date_column=DateOnly(col="reference_date"),
    date_format=DateFormat.YEAR_MD,
)


# ──────────────────────────────────────────────────────────────────────────────
# event_pipeline — piloto da arquitetura em estágios (staged pipeline)
#
# Variante padrão (check_update/download separados), sem partição —
# um único CSV. Tabela: test_dataset.test_event_pipeline.
# ──────────────────────────────────────────────────────────────────────────────

EVENT_PIPELINE_TABLE_ID = "test_event_pipeline"

# Recursos de pod pras etapas deste piloto — sobrescrevem o default do
# work pool (2 CPU / 4Gi de limite; 500m CPU / 1Gi de request).
#
# `memory_request` do check_update precisa ser explicitamente menor que o
# default do pool (1Gi) porque o Kubernetes rejeita um pod cujo request >
# limit — sem isso, ficaria maior que o `memory_limit` (512Mi) e o deploy
# falharia.
EVENT_PIPELINE_JOB_VARIABLES = {
    "check_update": {
        "cpu_limit": "500m",
        "memory_limit": "512Mi",
        "memory_request": "256Mi",
    },
    "extract_and_load": {
        "cpu_limit": "1",
        "memory_limit": "2Gi",
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# event_pipeline_partitioned — variante do piloto acima testando dados
# particionados (ano=/mes=), pra exercitar ExtractAndLoad.partition_folders
# e transfer_files_to_prod_flow(folders=...) de ponta a ponta — o piloto
# acima usa um único arquivo, nunca exercitou esse caminho. Mesmo
# dataset_id (test_dataset), tabela própria.
# ──────────────────────────────────────────────────────────────────────────────

EVENT_PIPELINE_PARTITIONED_TABLE_ID = "test_event_pipeline_partitioned"

# Mesmos tiers do event_pipeline — carga igualmente sintética/leve.
EVENT_PIPELINE_PARTITIONED_JOB_VARIABLES = EVENT_PIPELINE_JOB_VARIABLES
