{{
    config(
        schema="test_dataset",
        alias="test_event_pipeline",
        materialized="table",
    )
}}

-- Piloto da issue #1867: lê o CSV que flow_download_flow sobe pro staging
-- (upload_to_gcs) — cada rodada substitui a única linha pela data do
-- download mais recente.
select safe_cast(reference_date as date) as reference_date
from {{ set_datalake_project("test_dataset_staging.test_event_pipeline") }} as t
