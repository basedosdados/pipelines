{{
    config(
        schema="au_dcceew_greenhouse",
        alias="inventory_unfccc",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2029, "interval": 1},
        },
        cluster_by=["geography", "unfccc_level_1", "gas_level_0"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(geography as string) geography,
    safe_cast(unfccc_level_1 as string) unfccc_level_1,
    safe_cast(unfccc_level_2 as string) unfccc_level_2,
    safe_cast(unfccc_level_3 as string) unfccc_level_3,
    safe_cast(unfccc_level_4 as string) unfccc_level_4,
    safe_cast(unfccc_level_5 as string) unfccc_level_5,
    safe_cast(unfccc_level_6 as string) unfccc_level_6,
    safe_cast(unfccc_level_7 as string) unfccc_level_7,
    safe_cast(unfccc_level_8 as string) unfccc_level_8,
    safe_cast(unfccc_level_9 as string) unfccc_level_9,
    safe_cast(unfccc_level_10 as string) unfccc_level_10,
    safe_cast(gas_level_0 as string) gas_level_0,
    safe_cast(gas_level_1 as string) gas_level_1,
    safe_cast(gas_level_2 as string) gas_level_2,
    safe_cast(gas_level_3 as string) gas_level_3,
    safe_cast(emissions_gg as float64) emissions_gg
from {{ set_datalake_project("au_dcceew_greenhouse_staging.inventory_unfccc") }} as t
