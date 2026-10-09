{{
    config(
        alias="arrestee",
        schema="us_fbi_cde",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1991, "end": 2030, "interval": 1},
        },
        cluster_by=["state_abbr", "offense_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_abbr as string) state_abbr,
    safe_cast(ori as string) ori,
    safe_cast(arrestee_id as string) arrestee_id,
    safe_cast(incident_id as string) incident_id,
    safe_cast(arrest_group as string) arrest_group,
    safe_cast(arrestee_sequence_number as string) arrestee_sequence_number,
    safe_cast(arrest_date as date) arrest_date,
    safe_cast(arrest_type_code as string) arrest_type_code,
    safe_cast(multiple_arrestee_indicator as string) multiple_arrestee_indicator,
    safe_cast(offense_code as string) offense_code,
    safe_cast(age_code as string) age_code,
    safe_cast(age as int64) age,
    safe_cast(age_range_low as int64) age_range_low,
    safe_cast(age_range_high as int64) age_range_high,
    safe_cast(sex_code as string) sex_code,
    safe_cast(race_code as string) race_code,
    safe_cast(ethnicity_code as string) ethnicity_code,
    safe_cast(resident_status_code as string) resident_status_code,
    safe_cast(under_18_disposition_code as string) under_18_disposition_code,
    safe_cast(weapon_code as string) weapon_code
from {{ set_datalake_project("us_fbi_cde_staging.arrestee") }} as t
