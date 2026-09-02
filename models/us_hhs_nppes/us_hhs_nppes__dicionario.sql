{{ config(alias="dicionario", schema="us_hhs_nppes", materialized="table") }}

-- Atualizado em 2026-09-02
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from {{ set_datalake_project("us_hhs_nppes_staging.dicionario") }} as t
