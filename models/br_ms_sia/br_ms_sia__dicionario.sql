{{ config(alias="dicionario", schema="br_ms_sia") }}
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(
        case
            when cobertura_temporal = '(1).0'
            then '(1)'
            when cobertura_temporal = '-1.0'
            then '(1)'
            when cobertura_temporal is null
            then '(1)'
            else cobertura_temporal
        end as string
    ) as cobertura_temporal,
    safe_cast(valor as string) valor
from {{ set_datalake_project("br_ms_sia_staging.dicionario") }}
