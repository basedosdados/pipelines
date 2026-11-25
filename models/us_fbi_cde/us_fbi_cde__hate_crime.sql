{{
    config(
        alias="hate_crime",
        schema="us_fbi_cde",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1991, "end": 2030, "interval": 1},
        },
        cluster_by=["state_abbr", "ori"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_abbr as string) state_abbr,
    safe_cast(ori as string) ori,
    safe_cast(incident_id as string) incident_id,
    safe_cast(incident_date as date) incident_date,
    safe_cast(agency_name as string) agency_name,
    safe_cast(agency_unit as string) agency_unit,
    safe_cast(agency_type as string) agency_type,
    safe_cast(division_name as string) division_name,
    safe_cast(region_name as string) region_name,
    safe_cast(population_group_code as string) population_group_code,
    safe_cast(offense_name as string) offense_name,
    safe_cast(bias_description as string) bias_description,
    safe_cast(location_name as string) location_name,
    safe_cast(victim_types as string) victim_types,
    safe_cast(victim_count as int64) victim_count,
    safe_cast(individual_victim_count as int64) individual_victim_count,
    safe_cast(adult_victim_count as int64) adult_victim_count,
    safe_cast(juvenile_victim_count as int64) juvenile_victim_count,
    safe_cast(offender_count as int64) offender_count,
    safe_cast(adult_offender_count as int64) adult_offender_count,
    safe_cast(juvenile_offender_count as int64) juvenile_offender_count,
    safe_cast(offender_race as string) offender_race,
    safe_cast(offender_ethnicity as string) offender_ethnicity,
    safe_cast(multiple_offense_flag as string) multiple_offense_flag,
    safe_cast(multiple_bias_flag as string) multiple_bias_flag
from {{ set_datalake_project("us_fbi_cde_staging.hate_crime") }} as t
