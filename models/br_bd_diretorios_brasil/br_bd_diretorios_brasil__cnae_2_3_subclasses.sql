{{
    config(
        alias="cnae_2_3_subclasses",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}
select
    safe_cast(cnae_2_3 as string) as cnae_2_3_subclasses,
    safe_cast(descricao as string) as descricao,
    safe_cast(cnae_2 as string) as cnae_2,
    safe_cast(descricao_cane_2 as string) as descricao_cnae_2,
    safe_cast(grupo as string) as grupo,
    safe_cast(descricao_grupo as string) as descricao_grupo,
    safe_cast(divisao as string) as divisao,
    safe_cast(descricao_divisao as string) as descricao_divisao,
    safe_cast(secao as string) as secao,
    safe_cast(descricao_secao as string) as descricao_secao
from
    {{ set_datalake_project("br_bd_diretorios_brasil_staging.cnae_2_3_subclasses") }}
    as t
