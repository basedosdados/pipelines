{{
    config(
        schema="us_cms_open_payments",
        alias="covered_recipient_profile",
        materialized="table",
    )
}}


select
    safe_cast(profile_type as string) profile_type,
    safe_cast(covered_recipient_profile_id as string) covered_recipient_profile_id,
    safe_cast(associated_profile_id_1 as string) associated_profile_id_1,
    safe_cast(associated_profile_id_2 as string) associated_profile_id_2,
    safe_cast(covered_recipient_npi as string) covered_recipient_npi,
    safe_cast(first_name as string) first_name,
    safe_cast(middle_name as string) middle_name,
    safe_cast(last_name as string) last_name,
    safe_cast(name_suffix as string) name_suffix,
    safe_cast(alternate_first_name as string) alternate_first_name,
    safe_cast(alternate_middle_name as string) alternate_middle_name,
    safe_cast(alternate_last_name as string) alternate_last_name,
    safe_cast(alternate_name_suffix as string) alternate_name_suffix,
    safe_cast(address_line_1 as string) address_line_1,
    safe_cast(address_line_2 as string) address_line_2,
    safe_cast(city as string) city,
    safe_cast(state as string) state,
    safe_cast(zip_code as string) zip_code,
    safe_cast(country as string) country,
    safe_cast(province as string) province,
    safe_cast(primary_specialty as string) primary_specialty,
    safe_cast(taxonomy_1 as string) taxonomy_1,
    safe_cast(taxonomy_2 as string) taxonomy_2,
    safe_cast(taxonomy_3 as string) taxonomy_3,
    safe_cast(taxonomy_4 as string) taxonomy_4,
    safe_cast(taxonomy_5 as string) taxonomy_5,
    safe_cast(taxonomy_6 as string) taxonomy_6,
    safe_cast(license_state_1 as string) license_state_1,
    safe_cast(license_state_2 as string) license_state_2,
    safe_cast(license_state_3 as string) license_state_3,
    safe_cast(license_state_4 as string) license_state_4,
    safe_cast(license_state_5 as string) license_state_5
from
    {{ set_datalake_project("us_cms_open_payments_staging.covered_recipient_profile") }}
    as t
