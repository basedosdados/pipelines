{{
    config(
        schema="us_cms_open_payments",
        alias="ownership",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(record_id as string) record_id,
    safe_cast(physician_profile_id as string) physician_profile_id,
    safe_cast(physician_npi as string) physician_npi,
    safe_cast(reporting_entity_id as string) reporting_entity_id,
    safe_cast(change_type as string) change_type,
    safe_cast(physician_first_name as string) physician_first_name,
    safe_cast(physician_middle_name as string) physician_middle_name,
    safe_cast(physician_last_name as string) physician_last_name,
    safe_cast(physician_name_suffix as string) physician_name_suffix,
    safe_cast(recipient_address_line_1 as string) recipient_address_line_1,
    safe_cast(recipient_address_line_2 as string) recipient_address_line_2,
    safe_cast(recipient_city as string) recipient_city,
    safe_cast(recipient_state as string) recipient_state,
    safe_cast(recipient_zip_code as string) recipient_zip_code,
    safe_cast(recipient_country as string) recipient_country,
    safe_cast(recipient_province as string) recipient_province,
    safe_cast(recipient_postal_code as string) recipient_postal_code,
    safe_cast(physician_primary_type as string) physician_primary_type,
    safe_cast(physician_specialty as string) physician_specialty,
    safe_cast(amount_invested_total as float64) amount_invested_total,
    safe_cast(interest_value as float64) interest_value,
    safe_cast(interest_terms as string) interest_terms,
    safe_cast(submitting_entity_name as string) submitting_entity_name,
    safe_cast(reporting_entity_name as string) reporting_entity_name,
    safe_cast(reporting_entity_state as string) reporting_entity_state,
    safe_cast(reporting_entity_country as string) reporting_entity_country,
    safe_cast(dispute_status as string) dispute_status,
    safe_cast(interest_held_by_physician_or_family as string) interest_held_by_physician_or_family,
    safe_cast(publication_date as date) publication_date
from {{ set_datalake_project("us_cms_open_payments_staging.ownership") }} as t
