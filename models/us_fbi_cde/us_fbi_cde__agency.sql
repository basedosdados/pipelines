{{
    config(
        alias="agency",
        schema="us_fbi_cde",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1960, "end": 2030, "interval": 1},
        },
        cluster_by=["state_abbr", "ori"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(ori as string) ori,
    safe_cast(legacy_ori as string) legacy_ori,
    safe_cast(state_id as string) state_id,
    safe_cast(state_abbr as string) state_abbr,
    safe_cast(county_id as string) county_id,
    safe_cast(county_name as string) county_name,
    safe_cast(agency_name as string) agency_name,
    safe_cast(agency_unit as string) agency_unit,
    safe_cast(agency_type as string) agency_type,
    safe_cast(division_name as string) division_name,
    safe_cast(region_name as string) region_name,
    safe_cast(population_group_code as string) population_group_code,
    safe_cast(population_group_description as string) population_group_description,
    safe_cast(core_city_flag as string) core_city_flag,
    safe_cast(population as int64) population,
    safe_cast(officer_count as int64) officer_count,
    safe_cast(civilian_count as int64) civilian_count,
    safe_cast(employee_count as int64) employee_count,
    safe_cast(male_officer_count as int64) male_officer_count,
    safe_cast(male_civilian_count as int64) male_civilian_count,
    safe_cast(female_officer_count as int64) female_officer_count,
    safe_cast(female_civilian_count as int64) female_civilian_count,
    safe_cast(employee_per_1000_inhabitants as float64) employee_per_1000_inhabitants,
    safe_cast(summary_months_reported as int64) summary_months_reported,
    safe_cast(nibrs_months_reported as int64) nibrs_months_reported,
    safe_cast(nibrs_participated as string) nibrs_participated,
    safe_cast(nibrs_start_date as date) nibrs_start_date,
    safe_cast(covered_by_ori as string) covered_by_ori,
    safe_cast(officer_killed_felonious_count as int64) officer_killed_felonious_count,
    safe_cast(officer_killed_accidental_count as int64) officer_killed_accidental_count,
    safe_cast(officer_assaulted_count as int64) officer_assaulted_count
from {{ set_datalake_project("us_fbi_cde_staging.agency") }} as t
