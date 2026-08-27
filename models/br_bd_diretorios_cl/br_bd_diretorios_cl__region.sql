{{
    config(
        schema="br_bd_diretorios_cl",
        alias="region",
        materialized="table",
    )
}}

select
    safe_cast(id_region as string) id_region,
    safe_cast(nombre as string) nombre,
    safe_cast(nombre_completo as string) nombre_completo,
    safe_cast(sigla as string) sigla,
    safe_cast(numero_romano as string) numero_romano
from {{ set_datalake_project("br_bd_diretorios_cl_staging.region") }} as t
