{{
    config(
        alias="departement",
        schema="br_bd_diretorios_fr",
        materialized="table",
    )
}}
select
    safe_cast(id_departamento as string) id_departamento,
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(id_comuna_sede as string) id_comuna_sede,
    safe_cast(nome_departamento as string) nome_departamento
from {{ set_datalake_project("br_bd_diretorios_fr_staging.departamento") }} as t
