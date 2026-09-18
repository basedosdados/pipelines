{{
    config(
        schema="us_cms_open_payments",
        alias="general",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2030, "interval": 1},
        },
        cluster_by=["covered_recipient_profile_id", "reporting_entity_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(record_id as string) record_id,
    safe_cast(covered_recipient_profile_id as string) covered_recipient_profile_id,
    safe_cast(covered_recipient_npi as string) covered_recipient_npi,
    safe_cast(teaching_hospital_ccn as string) teaching_hospital_ccn,
    safe_cast(teaching_hospital_id as string) teaching_hospital_id,
    safe_cast(reporting_entity_id as string) reporting_entity_id,
    safe_cast(change_type as string) change_type,
    safe_cast(covered_recipient_type as string) covered_recipient_type,
    safe_cast(teaching_hospital_name as string) teaching_hospital_name,
    safe_cast(covered_recipient_first_name as string) covered_recipient_first_name,
    safe_cast(covered_recipient_middle_name as string) covered_recipient_middle_name,
    safe_cast(covered_recipient_last_name as string) covered_recipient_last_name,
    safe_cast(covered_recipient_name_suffix as string) covered_recipient_name_suffix,
    safe_cast(recipient_address_line_1 as string) recipient_address_line_1,
    safe_cast(recipient_address_line_2 as string) recipient_address_line_2,
    safe_cast(recipient_city as string) recipient_city,
    safe_cast(recipient_state as string) recipient_state,
    safe_cast(recipient_zip_code as string) recipient_zip_code,
    safe_cast(recipient_country as string) recipient_country,
    safe_cast(recipient_province as string) recipient_province,
    safe_cast(recipient_postal_code as string) recipient_postal_code,
    safe_cast(
        covered_recipient_primary_type_1 as string
    ) covered_recipient_primary_type_1,
    safe_cast(
        covered_recipient_primary_type_2 as string
    ) covered_recipient_primary_type_2,
    safe_cast(
        covered_recipient_primary_type_3 as string
    ) covered_recipient_primary_type_3,
    safe_cast(
        covered_recipient_primary_type_4 as string
    ) covered_recipient_primary_type_4,
    safe_cast(
        covered_recipient_primary_type_5 as string
    ) covered_recipient_primary_type_5,
    safe_cast(
        covered_recipient_primary_type_6 as string
    ) covered_recipient_primary_type_6,
    safe_cast(covered_recipient_specialty_1 as string) covered_recipient_specialty_1,
    safe_cast(covered_recipient_specialty_2 as string) covered_recipient_specialty_2,
    safe_cast(covered_recipient_specialty_3 as string) covered_recipient_specialty_3,
    safe_cast(covered_recipient_specialty_4 as string) covered_recipient_specialty_4,
    safe_cast(covered_recipient_specialty_5 as string) covered_recipient_specialty_5,
    safe_cast(covered_recipient_specialty_6 as string) covered_recipient_specialty_6,
    safe_cast(
        covered_recipient_license_state_1 as string
    ) covered_recipient_license_state_1,
    safe_cast(
        covered_recipient_license_state_2 as string
    ) covered_recipient_license_state_2,
    safe_cast(
        covered_recipient_license_state_3 as string
    ) covered_recipient_license_state_3,
    safe_cast(
        covered_recipient_license_state_4 as string
    ) covered_recipient_license_state_4,
    safe_cast(
        covered_recipient_license_state_5 as string
    ) covered_recipient_license_state_5,
    safe_cast(submitting_entity_name as string) submitting_entity_name,
    safe_cast(reporting_entity_name as string) reporting_entity_name,
    safe_cast(reporting_entity_state as string) reporting_entity_state,
    safe_cast(reporting_entity_country as string) reporting_entity_country,
    safe_cast(payment_amount_total as float64) payment_amount_total,
    safe_cast(payment_date as date) payment_date,
    safe_cast(payment_count as int64) payment_count,
    safe_cast(payment_form as string) payment_form,
    safe_cast(payment_nature as string) payment_nature,
    safe_cast(travel_city as string) travel_city,
    safe_cast(travel_state as string) travel_state,
    safe_cast(travel_country as string) travel_country,
    safe_cast(physician_ownership_indicator as string) physician_ownership_indicator,
    safe_cast(
        third_party_payment_recipient_indicator as string
    ) third_party_payment_recipient_indicator,
    safe_cast(third_party_entity_name as string) third_party_entity_name,
    safe_cast(charity_indicator as string) charity_indicator,
    safe_cast(
        third_party_equals_covered_recipient_indicator as string
    ) third_party_equals_covered_recipient_indicator,
    safe_cast(contextual_information as string) contextual_information,
    safe_cast(delay_in_publication_indicator as string) delay_in_publication_indicator,
    safe_cast(dispute_status as string) dispute_status,
    safe_cast(related_product_indicator as string) related_product_indicator,
    safe_cast(product_covered_indicator_1 as string) product_covered_indicator_1,
    safe_cast(product_type_1 as string) product_type_1,
    safe_cast(product_category_1 as string) product_category_1,
    safe_cast(product_name_1 as string) product_name_1,
    safe_cast(product_ndc_1 as string) product_ndc_1,
    safe_cast(product_pdi_1 as string) product_pdi_1,
    safe_cast(product_covered_indicator_2 as string) product_covered_indicator_2,
    safe_cast(product_type_2 as string) product_type_2,
    safe_cast(product_category_2 as string) product_category_2,
    safe_cast(product_name_2 as string) product_name_2,
    safe_cast(product_ndc_2 as string) product_ndc_2,
    safe_cast(product_pdi_2 as string) product_pdi_2,
    safe_cast(product_covered_indicator_3 as string) product_covered_indicator_3,
    safe_cast(product_type_3 as string) product_type_3,
    safe_cast(product_category_3 as string) product_category_3,
    safe_cast(product_name_3 as string) product_name_3,
    safe_cast(product_ndc_3 as string) product_ndc_3,
    safe_cast(product_pdi_3 as string) product_pdi_3,
    safe_cast(product_covered_indicator_4 as string) product_covered_indicator_4,
    safe_cast(product_type_4 as string) product_type_4,
    safe_cast(product_category_4 as string) product_category_4,
    safe_cast(product_name_4 as string) product_name_4,
    safe_cast(product_ndc_4 as string) product_ndc_4,
    safe_cast(product_pdi_4 as string) product_pdi_4,
    safe_cast(product_covered_indicator_5 as string) product_covered_indicator_5,
    safe_cast(product_type_5 as string) product_type_5,
    safe_cast(product_category_5 as string) product_category_5,
    safe_cast(product_name_5 as string) product_name_5,
    safe_cast(product_ndc_5 as string) product_ndc_5,
    safe_cast(product_pdi_5 as string) product_pdi_5,
    safe_cast(publication_date as date) publication_date
from {{ set_datalake_project("us_cms_open_payments_staging.general") }} as t
