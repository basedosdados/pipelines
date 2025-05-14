{{
    config(
        alias="cbo_2002",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}


select
    safe_cast(cbo_2002 as string) cbo_2002,
    safe_cast(initcap(descricao) as string) descricao,
    safe_cast(familia as string) familia,
    safe_cast(descricao_familia as string) descricao_familia,
    safe_cast(subgrupo as string) subgrupo,
    safe_cast(initcap(descricao_subgrupo) as string) descricao_subgrupo,
    safe_cast(subgrupo_principal as string) subgrupo_principal,
    safe_cast(
        initcap(descricao_subgrupo_principal) as string
    ) descricao_subgrupo_principal,
    safe_cast(grande_grupo as string) grande_grupo,
    safe_cast(initcap(descricao_grande_grupo) as string) descricao_grande_grupo,
    safe_cast(indicador_cbo_2002_ativa as int64) indicador_cbo_2002_ativa
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.cbo_2002") }} as t
