{{
    config(
        alias="etnia_indigena",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(id_etnia_indigena as string) as id_etnia_indigena,
    safe_cast(nome as string) as nome
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.etnia_indigena") }} as t
