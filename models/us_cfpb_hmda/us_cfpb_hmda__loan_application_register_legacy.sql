{{
    config(
        schema="us_cfpb_hmda",
        alias="loan_application_register_legacy",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2022, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_id as string) state_id,
    safe_cast(county_id as string) county_id,
    safe_cast(census_tract_id as string) census_tract_id,
    safe_cast(msa_md_id as string) msa_md_id,
    safe_cast(respondent_id as string) respondent_id,
    safe_cast(agency_code as string) agency_code,
    safe_cast(loan_type as string) loan_type,
    safe_cast(property_type as string) property_type,
    safe_cast(loan_purpose as string) loan_purpose,
    safe_cast(owner_occupancy as string) owner_occupancy,
    safe_cast(preapproval as string) preapproval,
    safe_cast(action_taken as string) action_taken,
    safe_cast(loan_amount as float64) loan_amount,
    safe_cast(income as float64) income,
    safe_cast(rate_spread as float64) rate_spread,
    safe_cast(hoepa_status as string) hoepa_status,
    safe_cast(lien_status as string) lien_status,
    safe_cast(purchaser_type as string) purchaser_type,
    safe_cast(applicant_ethnicity as string) applicant_ethnicity,
    safe_cast(co_applicant_ethnicity as string) co_applicant_ethnicity,
    safe_cast(applicant_race_1 as string) applicant_race_1,
    safe_cast(applicant_race_2 as string) applicant_race_2,
    safe_cast(applicant_race_3 as string) applicant_race_3,
    safe_cast(applicant_race_4 as string) applicant_race_4,
    safe_cast(applicant_race_5 as string) applicant_race_5,
    safe_cast(co_applicant_race_1 as string) co_applicant_race_1,
    safe_cast(co_applicant_race_2 as string) co_applicant_race_2,
    safe_cast(co_applicant_race_3 as string) co_applicant_race_3,
    safe_cast(co_applicant_race_4 as string) co_applicant_race_4,
    safe_cast(co_applicant_race_5 as string) co_applicant_race_5,
    safe_cast(applicant_sex as string) applicant_sex,
    safe_cast(co_applicant_sex as string) co_applicant_sex,
    safe_cast(denial_reason_1 as string) denial_reason_1,
    safe_cast(denial_reason_2 as string) denial_reason_2,
    safe_cast(denial_reason_3 as string) denial_reason_3,
    safe_cast(tract_population as int64) tract_population,
    safe_cast(
        tract_minority_population_percent as float64
    ) tract_minority_population_percent,
    safe_cast(hud_median_family_income as int64) hud_median_family_income,
    safe_cast(tract_to_msa_income_percentage as float64) tract_to_msa_income_percentage,
    safe_cast(tract_owner_occupied_units as int64) tract_owner_occupied_units,
    safe_cast(tract_one_to_four_family_homes as int64) tract_one_to_four_family_homes
from
    {{ set_datalake_project("us_cfpb_hmda_staging.loan_application_register_legacy") }}
    as t
