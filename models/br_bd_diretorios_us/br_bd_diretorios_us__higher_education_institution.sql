{{
    config(
        alias="higher_education_institution",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_institution as string) id_institution,
    safe_cast(id_opeid as string) id_opeid,
    safe_cast(name as string) name,
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(id_county as string) id_county,
    safe_cast(city as string) city,
    safe_cast(zip as string) zip,
    safe_cast(sector as string) sector,
    safe_cast(control as string) control,
    safe_cast(level as string) level,
    safe_cast(operational_status as string) operational_status
from
    {{
        set_datalake_project(
            "br_bd_diretorios_us_staging.higher_education_institution"
        )
    }} as t
