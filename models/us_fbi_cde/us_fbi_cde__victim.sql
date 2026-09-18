{{
    config(
        alias="victim",
        schema="us_fbi_cde",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1991, "end": 2030, "interval": 1},
        },
        cluster_by=["state_abbr"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_abbr as string) state_abbr,
    safe_cast(victim_id as string) victim_id,
    safe_cast(incident_id as string) incident_id,
    safe_cast(victim_sequence_number as string) victim_sequence_number,
    safe_cast(victim_type_code as string) victim_type_code,
    safe_cast(age_code as string) age_code,
    safe_cast(age as int64) age,
    safe_cast(age_range_low as int64) age_range_low,
    safe_cast(age_range_high as int64) age_range_high,
    safe_cast(sex_code as string) sex_code,
    safe_cast(race_code as string) race_code,
    safe_cast(ethnicity_code as string) ethnicity_code,
    safe_cast(resident_status_code as string) resident_status_code,
    safe_cast(assignment_type_code as string) assignment_type_code,
    safe_cast(activity_type_code as string) activity_type_code,
    safe_cast(injury_code as string) injury_code,
    safe_cast(injury_count as int64) injury_count
from {{ set_datalake_project("us_fbi_cde_staging.victim") }} as t
