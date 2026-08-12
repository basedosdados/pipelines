{{
    config(
        alias="comuna",
        schema="br_bd_diretorios_fr",
        materialized="table",
    )
}}
select
    safe_cast(id_comuna as string) id_comuna,
    safe_cast(id_departamento as string) id_departamento,
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(nome_comuna as string) nome_comuna,
    safe_cast(tipo_comuna as string) tipo_comuna
from {{ set_datalake_project("br_bd_diretorios_fr_staging.comuna") }} as t
