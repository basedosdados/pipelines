{{
    config(
        schema="us_cms_open_payments",
        alias="research_principal_investigator",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2030, "interval": 1},
        },
        cluster_by=["covered_recipient_profile_id", "record_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(record_id as string) record_id,
    safe_cast(covered_recipient_profile_id as string) covered_recipient_profile_id,
    safe_cast(covered_recipient_npi as string) covered_recipient_npi,
    safe_cast(principal_investigator_number as string) principal_investigator_number,
    safe_cast(first_name as string) first_name,
    safe_cast(middle_name as string) middle_name,
    safe_cast(last_name as string) last_name,
    safe_cast(name_suffix as string) name_suffix,
    safe_cast(address_line_1 as string) address_line_1,
    safe_cast(address_line_2 as string) address_line_2,
    safe_cast(city as string) city,
    safe_cast(state as string) state,
    safe_cast(zip_code as string) zip_code,
    safe_cast(country as string) country,
    safe_cast(province as string) province,
    safe_cast(postal_code as string) postal_code,
    safe_cast(primary_type_1 as string) primary_type_1,
    safe_cast(specialty_1 as string) specialty_1,
    safe_cast(license_state_1 as string) license_state_1,
    safe_cast(license_state_2 as string) license_state_2,
    safe_cast(license_state_3 as string) license_state_3,
    safe_cast(license_state_4 as string) license_state_4,
    safe_cast(license_state_5 as string) license_state_5,
    safe_cast(covered_recipient_type as string) covered_recipient_type,
    safe_cast(primary_type_2 as string) primary_type_2,
    safe_cast(primary_type_3 as string) primary_type_3,
    safe_cast(primary_type_4 as string) primary_type_4,
    safe_cast(primary_type_5 as string) primary_type_5,
    safe_cast(primary_type_6 as string) primary_type_6,
    safe_cast(specialty_2 as string) specialty_2,
    safe_cast(specialty_3 as string) specialty_3,
    safe_cast(specialty_4 as string) specialty_4,
    safe_cast(specialty_5 as string) specialty_5,
    safe_cast(specialty_6 as string) specialty_6
from
    {{
        set_datalake_project(
            "us_cms_open_payments_staging.research_principal_investigator"
        )
    }} as t
