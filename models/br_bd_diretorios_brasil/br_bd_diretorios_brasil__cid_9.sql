{{
    config(
        alias="cid_9",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(categoria as string) categoria, safe_cast(descricao as string) descricao
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.cid_9") }} as t
