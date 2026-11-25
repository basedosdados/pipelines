{{
    config(
        alias="naf_rev2",
        schema="br_bd_diretorios_fr",
        materialized="table",
    )
}}
select
    safe_cast(naf_rev2 as string) naf_rev2,
    safe_cast(descricao_naf_rev2 as string) descricao_naf_rev2,
    safe_cast(id_classe as string) id_classe,
    safe_cast(descricao_classe as string) descricao_classe,
    safe_cast(id_grupo as string) id_grupo,
    safe_cast(descricao_grupo as string) descricao_grupo,
    safe_cast(id_divisao as string) id_divisao,
    safe_cast(descricao_divisao as string) descricao_divisao,
    safe_cast(id_secao as string) id_secao,
    safe_cast(descricao_secao as string) descricao_secao
from {{ set_datalake_project("br_bd_diretorios_fr_staging.naf_rev2") }} as t
