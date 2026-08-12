{{
    config(
        alias="hs2017",
        schema="br_bd_diretorios_comercio_internacional",
        materialized="table",
    )
}}

select
    safe_cast(id_sh6 as string) id_sh6,
    safe_cast(id_sh4 as string) id_sh4,
    safe_cast(id_sh2 as string) id_sh2,
    safe_cast(nome_ingles as string) nome_ingles
from
    {{ set_datalake_project("br_bd_diretorios_comercio_internacional_staging.hs2017") }}
    as t
