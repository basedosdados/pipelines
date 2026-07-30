"""
Flows de teste para a função download_data_to_gcs.
"""

from prefect import flow

from pipelines.utils.tasks import run_dbt

DATASET_ID = "test_dataset"


# ──────────────────────────────────────────────────────────────────────────────
# Testes de download_data_to_gcs — um flow por condição de tamanho
#
# Cada flow chama run_dbt com target="prod", que ao final dispara
# download_data_to_gcs automaticamente. Os modelos dbt usam FARM_FINGERPRINT
# para gerar dados sintéticos com tamanhos previsíveis.
#
# Caso 1 — < 100 MB, sem bdpro_filter  → exporta open
# Caso 2 — < 100 MB, com bdpro_filter  → exporta open + BDPro (post-hook no .sql)
# Caso 3 — 100 MB-1 GB                 → exporta apenas BDPro
# Caso 4 — > 1 GB                      → sem export (retorna cedo)
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name="test_dataset: download_data_to_gcs (< 100 MB sem bdpro)",
    flow_run_name="test download: < 100 MB sem bdpro",
    log_prints=True,
)
def test_download_pequena_flow() -> None:
    run_dbt(
        dataset_id=DATASET_ID,
        table_id="tabela_pequena",
        dbt_command="run",
        target="prod",
    )


@flow(
    name="test_dataset: download_data_to_gcs (< 100 MB com bdpro)",
    flow_run_name="test download: < 100 MB com bdpro",
    log_prints=True,
)
def test_download_bdpro_flow() -> None:
    run_dbt(
        dataset_id=DATASET_ID,
        table_id="tabela_pequena_bdpro",
        dbt_command="run",
        target="prod",
    )


@flow(
    name="test_dataset: download_data_to_gcs (100 MB-1 GB)",
    flow_run_name="test download: 100 MB-1 GB",
    log_prints=True,
)
def test_download_media_flow() -> None:
    run_dbt(
        dataset_id=DATASET_ID,
        table_id="tabela_media",
        dbt_command="run",
        target="prod",
    )


@flow(
    name="test_dataset: download_data_to_gcs (> 1 GB)",
    flow_run_name="test download: > 1 GB",
    log_prints=True,
)
def test_download_grande_flow() -> None:
    run_dbt(
        dataset_id=DATASET_ID,
        table_id="tabela_grande",
        dbt_command="run",
        target="prod",
    )
