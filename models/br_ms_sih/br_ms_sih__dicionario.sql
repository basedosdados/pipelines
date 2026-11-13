{{ config(alias="dicionario", schema="br_ms_sih") }}

-- comentário para fazer a action disparar. pode ser apagado no futuro.
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from {{ set_datalake_project("br_ms_sih_staging.dicionario") }}
