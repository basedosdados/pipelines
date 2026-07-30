"""
Flows de teste para validar o pipeline completo e a função download_data_to_gcs.
"""

from prefect import flow

from pipelines.datasets.test_dataset.tasks import download_taxa_cambio
from pipelines.utils.tasks import run_dbt, upload_to_gcs

DATASET_ID = "test_dataset"
TABLE_ID = "taxa_cambio"


@flow(
    name="test_dataset: taxa_cambio (end-to-end)",
    flow_run_name="test end-to-end: taxa_cambio",
    log_prints=True,
)
def test_taxa_cambio_flow(
    n_days: int = 30,
    materialize_after_dump: bool = True,
    target: str = "prod",
) -> None:
    filepath = download_taxa_cambio(n_days=n_days)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
    )

    run_dbt(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dbt_command="run/test",
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=filepath,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name="basedosdados",
        dump_mode="overwrite",
    )

    run_dbt(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dbt_command="run",
        target=target,
    )


test_taxa_cambio_flow.deploy_schedules = []


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
