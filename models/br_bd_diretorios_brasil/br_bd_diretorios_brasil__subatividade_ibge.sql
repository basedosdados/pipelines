{{
    config(
        alias="subatividade_ibge",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(id_subatividade as string) id_subatividade,
    safe_cast(descricao as string) descricao
from
    {{ set_datalake_project("br_bd_diretorios_brasil_staging.subatividade_ibge") }} as t
