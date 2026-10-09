{{
    config(
        alias="categorie_juridique",
        schema="br_bd_diretorios_fr",
        materialized="table",
    )
}}
select
    safe_cast(categoria_juridica as string) categoria_juridica,
    safe_cast(descricao_categoria_juridica as string) descricao_categoria_juridica,
    safe_cast(id_nivel_2 as string) id_nivel_2,
    safe_cast(descricao_nivel_2 as string) descricao_nivel_2,
    safe_cast(id_nivel_1 as string) id_nivel_1,
    safe_cast(descricao_nivel_1 as string) descricao_nivel_1
from {{ set_datalake_project("br_bd_diretorios_fr_staging.categorie_juridique") }} as t
