{{
    config(
        schema="test_dataset",
        alias="test_event_pipeline_partitioned",
        materialized="table",
    )
}}

-- Variante particionada (ano=/mes=) do piloto da issue #1867 — testa
-- DownloadResult.partition_folders/transfer_files_to_prod_flow(folders=...)
-- promovendo só a fatia nova, não o staging inteiro. As colunas ano/mes
-- vêm explícitas no CSV (não extraídas do caminho Hive), igual ao dado
-- real de cada arquivo.
select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    safe_cast(reference_date as date) as reference_date
from
    {{ set_datalake_project("test_dataset_staging.test_event_pipeline_partitioned") }}
    as t
