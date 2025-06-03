{{ config(alias="dictionary", schema="world_iae_timss", materialized="table") }}

select
    safe_cast(id_tabela as string) as table_id,
    safe_cast(nome_coluna as string) as column_name,
    safe_cast(chave as string) as key,
    safe_cast(cobertura_temporal as string) as temporal_coverage,
    safe_cast(valor as string) as value
from {{ set_datalake_project("world_iae_timss_staging.dicionario") }} as t
