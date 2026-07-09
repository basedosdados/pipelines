{{
    config(
        alias="school",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_school as string) id_school,
    safe_cast(name as string) name,
    safe_cast(id_school_district as string) id_school_district,
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(id_county as string) id_county,
    safe_cast(type as string) type,
    safe_cast(level as string) level,
    safe_cast(charter as string) charter,
    safe_cast(operational_status as string) operational_status
from {{ set_datalake_project("br_bd_diretorios_us_staging.school") }} as t
