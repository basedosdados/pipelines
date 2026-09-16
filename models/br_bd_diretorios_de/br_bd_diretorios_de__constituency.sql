{{
    config(
        schema="br_bd_diretorios_de",
        alias="constituency",
        materialized="table",
    )
}}

select
    safe_cast(id_constituency as string) id_constituency,
    safe_cast(constituency_type as string) constituency_type,
    safe_cast(id_state as string) id_state,
    safe_cast(name as string) name
from {{ set_datalake_project("br_bd_diretorios_de_staging.constituency") }} as t
