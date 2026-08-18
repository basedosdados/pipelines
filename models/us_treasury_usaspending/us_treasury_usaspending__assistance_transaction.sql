{{
    config(
        schema="us_treasury_usaspending",
        alias="assistance_transaction",
        materialized="table",
        partition_by={
            "field": "fiscal_year",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2031, "interval": 1},
        },
        cluster_by=["awarding_agency_code", "cfda_number", "recipient_state_code"],
    )
}}


select
    safe_cast(fiscal_year as int64) fiscal_year,
    safe_cast(
        assistance_transaction_unique_key as string
    ) assistance_transaction_unique_key,
    safe_cast(assistance_award_unique_key as string) assistance_award_unique_key,
    safe_cast(award_id_fain as string) award_id_fain,
    safe_cast(modification_number as string) modification_number,
    safe_cast(award_id_uri as string) award_id_uri,
    safe_cast(sai_number as string) sai_number,
    safe_cast(federal_action_obligation as float64) federal_action_obligation,
    safe_cast(total_obligated_amount as float64) total_obligated_amount,
    safe_cast(
        total_outlayed_amount_for_overall_award as float64
    ) total_outlayed_amount_for_overall_award,
    safe_cast(
        indirect_cost_federal_share_amount as float64
    ) indirect_cost_federal_share_amount,
    safe_cast(non_federal_funding_amount as float64) non_federal_funding_amount,
    safe_cast(
        total_non_federal_funding_amount as float64
    ) total_non_federal_funding_amount,
    safe_cast(face_value_of_loan as float64) face_value_of_loan,
    safe_cast(original_loan_subsidy_cost as float64) original_loan_subsidy_cost,
    safe_cast(total_face_value_of_loan as float64) total_face_value_of_loan,
    safe_cast(total_loan_subsidy_cost as float64) total_loan_subsidy_cost,
    safe_cast(
        generated_pragmatic_obligations as float64
    ) generated_pragmatic_obligations,
    safe_cast(
        disaster_emergency_fund_codes_for_overall_award as string
    ) disaster_emergency_fund_codes_for_overall_award,
    safe_cast(
        outlayed_amount_from_covid19_supplementals_for_overall_award as float64
    ) outlayed_amount_from_covid19_supplementals_for_overall_award,
    safe_cast(
        obligated_amount_from_covid19_supplementals_for_overall_award as float64
    ) obligated_amount_from_covid19_supplementals_for_overall_award,
    safe_cast(
        outlayed_amount_from_iija_supplemental_for_overall_award as float64
    ) outlayed_amount_from_iija_supplemental_for_overall_award,
    safe_cast(
        obligated_amount_from_iija_supplemental_for_overall_award as float64
    ) obligated_amount_from_iija_supplemental_for_overall_award,
    safe_cast(action_date as date) action_date,
    safe_cast(
        period_of_performance_start_date as date
    ) period_of_performance_start_date,
    safe_cast(
        period_of_performance_current_end_date as date
    ) period_of_performance_current_end_date,
    safe_cast(awarding_agency_code as string) awarding_agency_code,
    safe_cast(awarding_agency_name as string) awarding_agency_name,
    safe_cast(awarding_sub_agency_code as string) awarding_sub_agency_code,
    safe_cast(awarding_sub_agency_name as string) awarding_sub_agency_name,
    safe_cast(awarding_office_code as string) awarding_office_code,
    safe_cast(awarding_office_name as string) awarding_office_name,
    safe_cast(funding_agency_code as string) funding_agency_code,
    safe_cast(funding_agency_name as string) funding_agency_name,
    safe_cast(funding_sub_agency_code as string) funding_sub_agency_code,
    safe_cast(funding_sub_agency_name as string) funding_sub_agency_name,
    safe_cast(funding_office_code as string) funding_office_code,
    safe_cast(funding_office_name as string) funding_office_name,
    safe_cast(
        treasury_accounts_funding_this_award as string
    ) treasury_accounts_funding_this_award,
    safe_cast(
        federal_accounts_funding_this_award as string
    ) federal_accounts_funding_this_award,
    safe_cast(
        object_classes_funding_this_award as string
    ) object_classes_funding_this_award,
    safe_cast(
        program_activities_funding_this_award as string
    ) program_activities_funding_this_award,
    safe_cast(recipient_uei as string) recipient_uei,
    safe_cast(recipient_duns as string) recipient_duns,
    safe_cast(recipient_name as string) recipient_name,
    safe_cast(recipient_name_raw as string) recipient_name_raw,
    safe_cast(recipient_parent_uei as string) recipient_parent_uei,
    safe_cast(recipient_parent_duns as string) recipient_parent_duns,
    safe_cast(recipient_parent_name as string) recipient_parent_name,
    safe_cast(recipient_parent_name_raw as string) recipient_parent_name_raw,
    safe_cast(recipient_country_code as string) recipient_country_code,
    safe_cast(recipient_country_name as string) recipient_country_name,
    safe_cast(recipient_address_line_1 as string) recipient_address_line_1,
    safe_cast(recipient_address_line_2 as string) recipient_address_line_2,
    safe_cast(recipient_city_code as string) recipient_city_code,
    safe_cast(recipient_city_name as string) recipient_city_name,
    safe_cast(
        prime_award_transaction_recipient_county_fips_code as string
    ) prime_award_transaction_recipient_county_fips_code,
    safe_cast(recipient_county_name as string) recipient_county_name,
    safe_cast(
        prime_award_transaction_recipient_state_fips_code as string
    ) prime_award_transaction_recipient_state_fips_code,
    safe_cast(recipient_state_code as string) recipient_state_code,
    safe_cast(recipient_state_name as string) recipient_state_name,
    safe_cast(recipient_zip_code as string) recipient_zip_code,
    safe_cast(recipient_zip_last_4_code as string) recipient_zip_last_4_code,
    safe_cast(
        prime_award_transaction_recipient_cd_original as string
    ) prime_award_transaction_recipient_cd_original,
    safe_cast(
        prime_award_transaction_recipient_cd_current as string
    ) prime_award_transaction_recipient_cd_current,
    safe_cast(recipient_foreign_city_name as string) recipient_foreign_city_name,
    safe_cast(
        recipient_foreign_province_name as string
    ) recipient_foreign_province_name,
    safe_cast(recipient_foreign_postal_code as string) recipient_foreign_postal_code,
    safe_cast(
        primary_place_of_performance_scope as string
    ) primary_place_of_performance_scope,
    safe_cast(
        primary_place_of_performance_country_code as string
    ) primary_place_of_performance_country_code,
    safe_cast(
        primary_place_of_performance_country_name as string
    ) primary_place_of_performance_country_name,
    safe_cast(
        primary_place_of_performance_code as string
    ) primary_place_of_performance_code,
    safe_cast(
        primary_place_of_performance_city_name as string
    ) primary_place_of_performance_city_name,
    safe_cast(
        prime_award_transaction_place_of_performance_county_fips_code as string
    ) prime_award_transaction_place_of_performance_county_fips_code,
    safe_cast(
        primary_place_of_performance_county_name as string
    ) primary_place_of_performance_county_name,
    safe_cast(
        prime_award_transaction_place_of_performance_state_fips_code as string
    ) prime_award_transaction_place_of_performance_state_fips_code,
    safe_cast(
        primary_place_of_performance_state_name as string
    ) primary_place_of_performance_state_name,
    safe_cast(
        primary_place_of_performance_zip_4 as string
    ) primary_place_of_performance_zip_4,
    safe_cast(
        prime_award_transaction_place_of_performance_cd_original as string
    ) prime_award_transaction_place_of_performance_cd_original,
    safe_cast(
        prime_award_transaction_place_of_performance_cd_current as string
    ) prime_award_transaction_place_of_performance_cd_current,
    safe_cast(
        primary_place_of_performance_foreign_location as string
    ) primary_place_of_performance_foreign_location,
    safe_cast(cfda_number as string) cfda_number,
    safe_cast(cfda_title as string) cfda_title,
    safe_cast(funding_opportunity_number as string) funding_opportunity_number,
    safe_cast(funding_opportunity_goals_text as string) funding_opportunity_goals_text,
    safe_cast(assistance_type_code as string) assistance_type_code,
    safe_cast(assistance_type_description as string) assistance_type_description,
    safe_cast(transaction_description as string) transaction_description,
    safe_cast(
        prime_award_base_transaction_description as string
    ) prime_award_base_transaction_description,
    safe_cast(business_funds_indicator_code as string) business_funds_indicator_code,
    safe_cast(
        business_funds_indicator_description as string
    ) business_funds_indicator_description,
    safe_cast(business_types_code as string) business_types_code,
    safe_cast(business_types_description as string) business_types_description,
    safe_cast(
        correction_delete_indicator_code as string
    ) correction_delete_indicator_code,
    safe_cast(
        correction_delete_indicator_description as string
    ) correction_delete_indicator_description,
    safe_cast(action_type_code as string) action_type_code,
    safe_cast(action_type_description as string) action_type_description,
    safe_cast(record_type_code as string) record_type_code,
    safe_cast(record_type_description as string) record_type_description,
    safe_cast(
        highly_compensated_officer_1_name as string
    ) highly_compensated_officer_1_name,
    safe_cast(
        highly_compensated_officer_1_amount as float64
    ) highly_compensated_officer_1_amount,
    safe_cast(
        highly_compensated_officer_2_name as string
    ) highly_compensated_officer_2_name,
    safe_cast(
        highly_compensated_officer_2_amount as float64
    ) highly_compensated_officer_2_amount,
    safe_cast(
        highly_compensated_officer_3_name as string
    ) highly_compensated_officer_3_name,
    safe_cast(
        highly_compensated_officer_3_amount as float64
    ) highly_compensated_officer_3_amount,
    safe_cast(
        highly_compensated_officer_4_name as string
    ) highly_compensated_officer_4_name,
    safe_cast(
        highly_compensated_officer_4_amount as float64
    ) highly_compensated_officer_4_amount,
    safe_cast(
        highly_compensated_officer_5_name as string
    ) highly_compensated_officer_5_name,
    safe_cast(
        highly_compensated_officer_5_amount as float64
    ) highly_compensated_officer_5_amount,
    safe_cast(usaspending_permalink as string) usaspending_permalink,
    safe_cast(initial_report_date as timestamp) initial_report_date,
    safe_cast(last_modified_date as timestamp) last_modified_date
from
    {{ set_datalake_project("us_treasury_usaspending_staging.assistance_transaction") }}
    as t
