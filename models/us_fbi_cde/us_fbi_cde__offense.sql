{{
    config(
        alias="offense",
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
    safe_cast(offense_id as string) offense_id,
    safe_cast(incident_id as string) incident_id,
    safe_cast(offense_code as string) offense_code,
    safe_cast(attempt_complete_flag as string) attempt_complete_flag,
    safe_cast(location_code as string) location_code,
    safe_cast(premises_entered_count as int64) premises_entered_count,
    safe_cast(method_entry_code as string) method_entry_code,
    safe_cast(weapon_code as string) weapon_code,
    safe_cast(weapon_count as int64) weapon_count,
    safe_cast(bias_motivation_code as string) bias_motivation_code,
    safe_cast(bias_motivation_count as int64) bias_motivation_count
from {{ set_datalake_project("us_fbi_cde_staging.offense") }} as t
