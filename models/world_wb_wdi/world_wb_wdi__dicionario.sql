{{ config(schema="world_wb_wdi", alias="dicionario", materialized="table") }}


select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{ set_datalake_project("world_wb_wdi_staging.dicionario") }} as t
