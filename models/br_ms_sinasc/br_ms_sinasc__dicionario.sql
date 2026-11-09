{{ config(alias="dicionario", schema="br_ms_sinasc") }}
-- Dicionário de dados do SINASC
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{ set_datalake_project("br_ms_sinasc_staging.dicionario") }} as t
-- códigos 8xx (uf) saíram de id_pais_mae
where not (coluna = 'id_pais_mae' and chave like '8%')
