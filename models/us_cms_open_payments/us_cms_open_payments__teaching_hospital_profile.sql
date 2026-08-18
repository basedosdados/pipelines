{{
    config(
        schema="us_cms_open_payments",
        alias="teaching_hospital_profile",
        materialized="table",
    )
}}


select
    safe_cast(teaching_hospital_ccn as string) teaching_hospital_ccn,
    safe_cast(name as string) name,
    safe_cast(address_line_1 as string) address_line_1,
    safe_cast(address_line_2 as string) address_line_2,
    safe_cast(city as string) city,
    safe_cast(state as string) state,
    safe_cast(zip_code as string) zip_code,
    safe_cast(alternate_name_1 as string) alternate_name_1,
    safe_cast(alternate_name_2 as string) alternate_name_2,
    safe_cast(alternate_name_3 as string) alternate_name_3,
    safe_cast(alternate_name_4 as string) alternate_name_4,
    safe_cast(alternate_name_5 as string) alternate_name_5
from {{ set_datalake_project("us_cms_open_payments_staging.teaching_hospital_profile") }} as t
