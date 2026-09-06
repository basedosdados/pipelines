{{
    config(
        schema="us_nchs_vital_statistics",
        alias="death",
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
    safe_cast(death_month as int64) death_month,
    safe_cast(death_day_of_week as string) death_day_of_week,
    safe_cast(state_residence_id as string) state_residence_id,
    safe_cast(county_residence_id as string) county_residence_id,
    safe_cast(residence_status as string) residence_status,
    safe_cast(age_detail_code as string) age_detail_code,
    safe_cast(age_years as int64) age_years,
    safe_cast(age_recode_27 as string) age_recode_27,
    safe_cast(age_recode_12 as string) age_recode_12,
    safe_cast(sex as string) sex,
    safe_cast(race_code as string) race_code,
    safe_cast(race_recode_3 as string) race_recode_3,
    safe_cast(race_recode_5 as string) race_recode_5,
    safe_cast(race_bridged_flag as string) race_bridged_flag,
    safe_cast(hispanic_origin_code as string) hispanic_origin_code,
    safe_cast(hispanic_origin_race_recode as string) hispanic_origin_race_recode,
    safe_cast(
        hispanic_origin_race_recode_1997 as string
    ) hispanic_origin_race_recode_1997,
    safe_cast(education_years as int64) education_years,
    safe_cast(education_code as string) education_code,
    safe_cast(education_reporting_flag as string) education_reporting_flag,
    safe_cast(marital_status as string) marital_status,
    safe_cast(place_of_death as string) place_of_death,
    safe_cast(manner_of_death as string) manner_of_death,
    safe_cast(underlying_cause_code as string) underlying_cause_code,
    safe_cast(icd_revision as string) icd_revision,
    safe_cast(cause_recode_113 as string) cause_recode_113,
    safe_cast(cause_recode_358 as string) cause_recode_358,
    safe_cast(cause_recode_39 as string) cause_recode_39,
    safe_cast(cause_recode_72 as string) cause_recode_72,
    safe_cast(record_axis_condition_count as int64) record_axis_condition_count,
    safe_cast(autopsy as string) autopsy,
    safe_cast(injury_at_work as string) injury_at_work
from {{ set_datalake_project("us_nchs_vital_statistics_staging.death") }} as t
