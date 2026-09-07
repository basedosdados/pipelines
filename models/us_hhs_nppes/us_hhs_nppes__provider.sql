{{
    config(
        schema="us_hhs_nppes",
        alias="provider",
        materialized="incremental",
        partition_by={
            "field": "extraction_date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

-- Atualizado em 2026-09-02
select
    safe_cast(extraction_date as date) extraction_date,
    safe_cast(npi as string) npi,
    safe_cast(replacement_npi as string) replacement_npi,
    safe_cast(entity_type_code as string) entity_type_code,
    safe_cast(organization_name as string) organization_name,
    safe_cast(last_name as string) last_name,
    safe_cast(first_name as string) first_name,
    safe_cast(middle_name as string) middle_name,
    safe_cast(name_prefix as string) name_prefix,
    safe_cast(name_suffix as string) name_suffix,
    safe_cast(credential as string) credential,
    safe_cast(
        other_organization_name_type_code as string
    ) other_organization_name_type_code,
    safe_cast(other_last_name as string) other_last_name,
    safe_cast(other_first_name as string) other_first_name,
    safe_cast(other_middle_name as string) other_middle_name,
    safe_cast(other_name_prefix as string) other_name_prefix,
    safe_cast(other_name_suffix as string) other_name_suffix,
    safe_cast(other_credential as string) other_credential,
    safe_cast(other_last_name_type_code as string) other_last_name_type_code,
    safe_cast(sex_code as string) sex_code,
    safe_cast(is_sole_proprietor as string) is_sole_proprietor,
    safe_cast(is_organization_subpart as string) is_organization_subpart,
    safe_cast(
        parent_organization_legal_business_name as string
    ) parent_organization_legal_business_name,
    safe_cast(mailing_address_line_1 as string) mailing_address_line_1,
    safe_cast(mailing_address_line_2 as string) mailing_address_line_2,
    safe_cast(mailing_address_city as string) mailing_address_city,
    safe_cast(mailing_address_state as string) mailing_address_state,
    safe_cast(mailing_address_postal_code as string) mailing_address_postal_code,
    safe_cast(mailing_address_country_code as string) mailing_address_country_code,
    safe_cast(
        mailing_address_telephone_number as string
    ) mailing_address_telephone_number,
    safe_cast(mailing_address_fax_number as string) mailing_address_fax_number,
    safe_cast(practice_address_line_1 as string) practice_address_line_1,
    safe_cast(practice_address_line_2 as string) practice_address_line_2,
    safe_cast(practice_address_city as string) practice_address_city,
    safe_cast(practice_address_state as string) practice_address_state,
    safe_cast(practice_address_postal_code as string) practice_address_postal_code,
    safe_cast(practice_address_country_code as string) practice_address_country_code,
    safe_cast(
        practice_address_telephone_number as string
    ) practice_address_telephone_number,
    safe_cast(practice_address_fax_number as string) practice_address_fax_number,
    safe_cast(primary_taxonomy_code as string) primary_taxonomy_code,
    safe_cast(authorized_official_last_name as string) authorized_official_last_name,
    safe_cast(authorized_official_first_name as string) authorized_official_first_name,
    safe_cast(
        authorized_official_middle_name as string
    ) authorized_official_middle_name,
    safe_cast(
        authorized_official_name_prefix as string
    ) authorized_official_name_prefix,
    safe_cast(
        authorized_official_name_suffix as string
    ) authorized_official_name_suffix,
    safe_cast(authorized_official_credential as string) authorized_official_credential,
    safe_cast(
        authorized_official_title_or_position as string
    ) authorized_official_title_or_position,
    safe_cast(
        authorized_official_telephone_number as string
    ) authorized_official_telephone_number,
    safe_cast(enumeration_date as date) enumeration_date,
    safe_cast(last_update_date as date) last_update_date,
    safe_cast(certification_date as date) certification_date,
    safe_cast(deactivation_date as date) deactivation_date,
    safe_cast(reactivation_date as date) reactivation_date
from {{ set_datalake_project("us_hhs_nppes_staging.provider") }} as t
{% if is_incremental() %}
    where
        safe_cast(extraction_date as date)
        > (select max(extraction_date) from {{ this }})
{% endif %}
