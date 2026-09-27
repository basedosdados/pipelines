{{ config(alias="dicionario", schema="us_irs_form990", materialized="table") }}

-- Atualizado em 2026-09-03
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from {{ set_datalake_project("us_irs_form990_staging.dicionario") }} as t
