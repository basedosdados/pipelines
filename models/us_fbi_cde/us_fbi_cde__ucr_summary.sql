{{
    config(
        alias="ucr_summary",
        schema="us_fbi_cde",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1985, "end": 2030, "interval": 1},
        },
        cluster_by=["state_abbr", "ori", "offense_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_abbr as string) state_abbr,
    safe_cast(ori as string) ori,
    safe_cast(legacy_ori as string) legacy_ori,
    safe_cast(record_number as string) record_number,
    safe_cast(month as int64) month,
    safe_cast(offense_code as string) offense_code,
    safe_cast(actual_count as int64) actual_count,
    safe_cast(unfounded_count as int64) unfounded_count,
    safe_cast(cleared_count as int64) cleared_count,
    safe_cast(juvenile_cleared_count as int64) juvenile_cleared_count,
    safe_cast(record_type_code as string) record_type_code,
    safe_cast(breakdown_reported_flag as string) breakdown_reported_flag
from {{ set_datalake_project("us_fbi_cde_staging.ucr_summary") }} as t
