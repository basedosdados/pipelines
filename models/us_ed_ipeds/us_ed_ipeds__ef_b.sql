{{
    config(
        schema="us_ed_ipeds",
        alias="ef_b",
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
    safe_cast(efbage as float64) efbage,
    safe_cast(line as int64) line,
    safe_cast(lstudy as float64) lstudy,
    safe_cast(xefage01 as string) xefage01,
    safe_cast(efage01 as float64) efage01,
    safe_cast(xefage02 as string) xefage02,
    safe_cast(efage02 as float64) efage02,
    safe_cast(xefage03 as string) xefage03,
    safe_cast(efage03 as float64) efage03,
    safe_cast(xefage04 as string) xefage04,
    safe_cast(efage04 as float64) efage04,
    safe_cast(xefage05 as string) xefage05,
    safe_cast(efage05 as float64) efage05,
    safe_cast(xefage06 as string) xefage06,
    safe_cast(efage06 as float64) efage06,
    safe_cast(xefage07 as string) xefage07,
    safe_cast(efage07 as float64) efage07,
    safe_cast(xefage08 as string) xefage08,
    safe_cast(efage08 as float64) efage08,
    safe_cast(xefage09 as string) xefage09,
    safe_cast(efage09 as float64) efage09,
    safe_cast(section as float64) section
from {{ set_datalake_project("us_ed_ipeds_staging.ef_b") }} as t
