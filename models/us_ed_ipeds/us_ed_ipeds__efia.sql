{{
    config(
        schema="us_ed_ipeds",
        alias="efia",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2002, "end": 2030, "interval": 1},
        },
        cluster_by=["unitid"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(unitid as int64) unitid,
    safe_cast(xcdactua as string) xcdactua,
    safe_cast(cdactua as float64) cdactua,
    safe_cast(xcnactua as string) xcnactua,
    safe_cast(cnactua as float64) cnactua,
    safe_cast(xcdactga as string) xcdactga,
    safe_cast(cdactga as float64) cdactga,
    safe_cast(xefteug as string) xefteug,
    safe_cast(efteug as float64) efteug,
    safe_cast(xeftegd as string) xeftegd,
    safe_cast(eftegd as float64) eftegd,
    safe_cast(xfteug as string) xfteug,
    safe_cast(fteug as float64) fteug,
    safe_cast(xftegd as string) xftegd,
    safe_cast(ftegd as float64) ftegd,
    safe_cast(xftedpp as string) xftedpp,
    safe_cast(ftedpp as string) ftedpp,
    safe_cast(acttype as float64) acttype
from
    {{ set_datalake_project("us_ed_ipeds_staging.efia") }} as t
    -- rematerialize us_ed_ipeds (table-approve re-trigger)
