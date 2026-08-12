{{
    config(
        schema="br_bd_diretorios_mx",
        alias="municipio",
        materialized="table",
    )
}}

select
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_estado as string) id_estado,
    safe_cast(nombre as string) nombre
from
    {{ set_datalake_project("br_bd_diretorios_mx_staging.municipio") }}
    as t
