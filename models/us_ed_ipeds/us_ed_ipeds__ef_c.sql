{{
    config(
        schema="us_ed_ipeds",
        alias="ef_c",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2030, "interval": 1},
        },
        cluster_by=["unitid"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(unitid as int64) unitid,
    safe_cast(efcstate as float64) efcstate,
    safe_cast(line as int64) line,
    safe_cast(xefres01 as string) xefres01,
    safe_cast(efres01 as int64) efres01,
    safe_cast(xefres02 as string) xefres02,
    safe_cast(efres02 as float64) efres02
from
    {{ set_datalake_project("us_ed_ipeds_staging.ef_c") }} as t
    -- rematerialize us_ed_ipeds (table-approve re-trigger)
