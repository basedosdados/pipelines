{{ config(alias="dicionario", schema="br_inep_censo_escolar") }}
-- Dicionário de dados do Censo Escolar
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{ set_datalake_project("br_inep_censo_escolar_staging.dicionario") }} as t
