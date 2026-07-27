{{
    config(
        schema="br_bd_diretorios_in",
        alias="state",
        materialized="table",
    )
}}


select
    safe_cast(acronym as string) acronym,
    safe_cast(name as string) name,
    safe_cast(iso_3166_2 as string) iso_3166_2,
    safe_cast(type as string) type,
    safe_cast(is_current as bool) is_current,
    safe_cast(successor_acronym as string) successor_acronym
from {{ set_datalake_project("br_bd_diretorios_in_staging.state") }} as t
