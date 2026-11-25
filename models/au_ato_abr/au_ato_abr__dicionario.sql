{{ config(alias="dicionario", schema="au_ato_abr", materialized="table") }}

-- Atualizado em 2026-08-14
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from {{ set_datalake_project("au_ato_abr_staging.dicionario") }} as t
