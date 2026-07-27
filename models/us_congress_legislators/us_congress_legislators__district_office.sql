{{
    config(
        alias="district_office",
        schema="us_congress_legislators",
        materialized="table",
    )
}}
select
    safe_cast(id_office as string) id_office,
    safe_cast(id_bioguide as string) id_bioguide,
    safe_cast(building as string) building,
    safe_cast(address as string) address,
    safe_cast(suite as string) suite,
    safe_cast(city as string) city,
    safe_cast(state as string) state,
    safe_cast(zip_code as string) zip_code,
    safe_cast(phone as string) phone,
    safe_cast(fax as string) fax,
    safe_cast(hours as string) hours,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude
from {{ set_datalake_project("us_congress_legislators_staging.district_office") }} as t
