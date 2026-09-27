{{
    config(
        schema="au_dcceew_greenhouse",
        alias="inventory_anzsic",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2029, "interval": 1},
        },
        cluster_by=["geography", "anzsic_level_1", "gas_level_0"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(geography as string) geography,
    safe_cast(anzsic_level_1 as string) anzsic_level_1,
    safe_cast(anzsic_level_2 as string) anzsic_level_2,
    safe_cast(anzsic_level_3 as string) anzsic_level_3,
    safe_cast(gas_level_0 as string) gas_level_0,
    safe_cast(gas_level_1 as string) gas_level_1,
    safe_cast(gas_level_2 as string) gas_level_2,
    safe_cast(gas_level_3 as string) gas_level_3,
    safe_cast(emissions_gg as float64) emissions_gg
from {{ set_datalake_project("au_dcceew_greenhouse_staging.inventory_anzsic") }} as t
