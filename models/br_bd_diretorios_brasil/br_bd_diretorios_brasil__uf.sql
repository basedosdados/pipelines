{{
    config(
        alias="uf",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(id_uf as string) id_uf,
    safe_cast(sigla as string) sigla,
    safe_cast(nome as string) nome,
    safe_cast(regiao as string) regiao
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.uf") }} as t
