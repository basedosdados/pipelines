{{
    config(
        alias="sitc",
        schema="br_bd_diretorios_comercio_internacional",
        materialized="table",
    )
}}

select
    safe_cast(id_sitc4 as string) id_sitc4,
    safe_cast(id_sitc2 as string) id_sitc2,
    safe_cast(id_sitc1 as string) id_sitc1,
    safe_cast(nome_ingles as string) nome_ingles,
    safe_cast(nome_curto_ingles as string) nome_curto_ingles
from
    {{ set_datalake_project("br_bd_diretorios_comercio_internacional_staging.sitc") }}
    as t
