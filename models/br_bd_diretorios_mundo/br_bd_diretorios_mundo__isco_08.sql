{{ config(alias="isco_08", schema="br_bd_diretorios_mundo", materialized="table") }}

select
    safe_cast(id_isco_08 as string) id_isco_08,
    safe_cast(id_isco_08_grande_grupo as string) id_isco_08_grande_grupo,
    safe_cast(id_isco_08_subgrupo_principal as string) id_isco_08_subgrupo_principal,
    safe_cast(id_isco_08_subgrupo as string) id_isco_08_subgrupo,
    safe_cast(nivel as string) nivel,
    safe_cast(nome_en as string) nome_en
from {{ set_datalake_project("br_bd_diretorios_mundo_staging.isco_08") }} as t
