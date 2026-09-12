{{
    config(
        schema="us_ed_nces_ccd",
        alias="staff",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1986, "end": 2030, "interval": 1},
        },
        cluster_by=["state_id", "agency_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(agency_id as string) agency_id,
    safe_cast(state_id as string) state_id,
    safe_cast(staff_category as string) staff_category,
    safe_cast(staff_fte as float64) staff_fte
from {{ set_datalake_project("us_ed_nces_ccd_staging.staff") }} as t
