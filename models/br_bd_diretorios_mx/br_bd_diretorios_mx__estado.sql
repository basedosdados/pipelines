{{
    config(
        schema="br_bd_diretorios_mx",
        alias="estado",
        materialized="table",
    )
}}

select
    safe_cast(id_estado as string) id_estado,
    safe_cast(nombre as string) nombre,
    safe_cast(abreviatura as string) abreviatura
from {{ set_datalake_project("br_bd_diretorios_mx_staging.estado") }} as t
