{{ config(alias="isic_4", schema="br_bd_diretorios_mundo", materialized="table") }}

select
    safe_cast(id_isic_4 as string) id_isic_4,
    safe_cast(id_isic_4_secao as string) id_isic_4_secao,
    safe_cast(id_isic_4_divisao as string) id_isic_4_divisao,
    safe_cast(id_isic_4_grupo as string) id_isic_4_grupo,
    safe_cast(nivel as string) nivel,
    safe_cast(nome_en as string) nome_en
from {{ set_datalake_project("br_bd_diretorios_mundo_staging.isic_4") }} as t
