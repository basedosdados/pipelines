{{
    config(
        alias="state",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation as string) abbreviation,
    safe_cast(name as string) name,
    safe_cast(id_gnis as string) id_gnis,
    safe_cast(id_census as string) id_census,
    safe_cast(id_icpsr as string) id_icpsr,
    safe_cast(id_region as string) id_region,
    safe_cast(name_region as string) name_region,
    safe_cast(id_division as string) id_division,
    safe_cast(name_division as string) name_division,
    safe_cast(type as string) type
from {{ set_datalake_project("br_bd_diretorios_us_staging.state") }} as t
