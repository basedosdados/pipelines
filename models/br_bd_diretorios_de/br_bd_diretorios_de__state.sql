{{
    config(
        schema="br_bd_diretorios_de",
        alias="state",
        materialized="table",
    )
}}

select
    safe_cast(id_state as string) id_state,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(name as string) name,
    safe_cast(name_en as string) name_en
from {{ set_datalake_project("br_bd_diretorios_de_staging.state") }} as t
