{{
    config(
        schema="us_ed_nces_ccd",
        alias="school_enrollment",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1986, "end": 2030, "interval": 1},
        },
        cluster_by=["state_id", "school_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(school_id as string) school_id,
    safe_cast(agency_id as string) agency_id,
    safe_cast(state_id as string) state_id,
    safe_cast(grade as string) grade,
    safe_cast(race as string) race,
    safe_cast(sex as string) sex,
    safe_cast(enrollment as int64) enrollment
from {{ set_datalake_project("us_ed_nces_ccd_staging.school_enrollment") }} as t
