{{
    config(
        schema="us_nchs_vital_statistics",
        alias="birth",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1968, "end": 2029, "interval": 1},
        },
        cluster_by=["state_residence_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(birth_month as int64) birth_month,
    safe_cast(birth_day_of_week as string) birth_day_of_week,
    safe_cast(state_residence_id as string) state_residence_id,
    safe_cast(county_residence_id as string) county_residence_id,
    safe_cast(residence_status as string) residence_status,
    safe_cast(record_weight as int64) record_weight,
    safe_cast(
        county_residence_population_code as string
    ) county_residence_population_code,
    safe_cast(mother_age_years as int64) mother_age_years,
    safe_cast(mother_age_recode_9 as string) mother_age_recode_9,
    safe_cast(mother_education_years as int64) mother_education_years,
    safe_cast(mother_education_code as string) mother_education_code,
    safe_cast(mother_race_code as string) mother_race_code,
    safe_cast(mother_race_bridged_code as string) mother_race_bridged_code,
    safe_cast(mother_race_recode_6 as string) mother_race_recode_6,
    safe_cast(mother_race_recode_31 as string) mother_race_recode_31,
    safe_cast(mother_hispanic_origin_code as string) mother_hispanic_origin_code,
    safe_cast(mother_race_hispanic_code as string) mother_race_hispanic_code,
    safe_cast(mother_nativity_code as string) mother_nativity_code,
    safe_cast(mother_marital_status as string) mother_marital_status,
    safe_cast(father_age_years as int64) father_age_years,
    safe_cast(father_race_code as string) father_race_code,
    safe_cast(father_race_recode_6 as string) father_race_recode_6,
    safe_cast(live_birth_order as int64) live_birth_order,
    safe_cast(total_birth_order as int64) total_birth_order,
    safe_cast(gestation_weeks as int64) gestation_weeks,
    safe_cast(gestation_recode_3 as string) gestation_recode_3,
    safe_cast(birth_weight_grams as int64) birth_weight_grams,
    safe_cast(birth_weight_recode_4 as string) birth_weight_recode_4,
    safe_cast(prenatal_care_month_began as int64) prenatal_care_month_began,
    safe_cast(prenatal_visits as int64) prenatal_visits,
    safe_cast(sex as string) sex,
    safe_cast(plurality as string) plurality,
    safe_cast(delivery_method_recode as string) delivery_method_recode,
    safe_cast(birth_attendant as string) birth_attendant,
    safe_cast(apgar_score_5min as int64) apgar_score_5min
from {{ set_datalake_project("us_nchs_vital_statistics_staging.birth") }} as t
