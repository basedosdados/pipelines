{{
    config(
        alias="natureza_juridica",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}
select
    safe_cast(id_natureza_juridica as string) as id_natureza_juridica,
    safe_cast(descricao as string) as descricao,
    safe_cast(escopo as string) as escopo
from
    {{ set_datalake_project("br_bd_diretorios_brasil_staging.natureza_juridica") }} as t
