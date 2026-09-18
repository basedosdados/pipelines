{{
    config(
        alias="congressional_district_119",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_congressional_district as string) id_congressional_district,
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(district as string) district,
    safe_cast(type as string) type
from
    {{ set_datalake_project("br_bd_diretorios_us_staging.congressional_district_119") }}
    as t
