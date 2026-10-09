{{
    config(
        alias="incident",
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
    safe_cast(incident_hour as string) incident_hour,
    safe_cast(report_date_flag as string) report_date_flag,
    safe_cast(cargo_theft_flag as string) cargo_theft_flag,
    safe_cast(cleared_except_code as string) cleared_except_code,
    safe_cast(cleared_except_date as date) cleared_except_date,
    safe_cast(incident_status as string) incident_status,
    safe_cast(submission_date as date) submission_date
from {{ set_datalake_project("us_fbi_cde_staging.incident") }} as t
