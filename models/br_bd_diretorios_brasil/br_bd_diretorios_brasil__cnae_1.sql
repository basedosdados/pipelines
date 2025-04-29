{{
    config(
        alias="cnae_1",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}
select
    safe_cast(replace(replace(t.cnae_1, '.', ''), '-', '') as string) as cnae_1,
    safe_cast(t.descricao as string) as descricao,
    safe_cast(t.grupo as string) as grupo,
    safe_cast(t.descricao_grupo as string) as descricao_grupo,
    safe_cast(t.divisao as string) as divisao,
    safe_cast(t.descricao_divisao as string) as descricao_divisao,
    safe_cast(t.secao as string) as secao,
    safe_cast(t.descricao_secao as string) as descricao_secao
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.cnae_1") }} as t
