{{
    config(
        schema="au_dcceew_greenhouse",
        alias="inventory_scope2",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2029, "interval": 1},
        },
        cluster_by=["geography", "scopetwo_level_1"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(geography as string) geography,
    safe_cast(scopetwo_level_1 as string) scopetwo_level_1,
    safe_cast(scopetwo_level_2 as string) scopetwo_level_2,
    safe_cast(emissions_gg as float64) emissions_gg
from {{ set_datalake_project("au_dcceew_greenhouse_staging.inventory_scope2") }} as t
