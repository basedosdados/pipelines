{{
    config(
        schema="au_abs_population",
        alias="regional_lga",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2001, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(lga_id as string) lga_id,
    safe_cast(lga_name as string) lga_name,
    safe_cast(state_id as string) state_id,
    safe_cast(erp as int64) erp,
    safe_cast(births as int64) births,
    safe_cast(deaths as int64) deaths,
    safe_cast(natural_increase as int64) natural_increase,
    safe_cast(internal_arrivals as int64) internal_arrivals,
    safe_cast(internal_departures as int64) internal_departures,
    safe_cast(net_internal_migration as int64) net_internal_migration,
    safe_cast(overseas_arrivals as int64) overseas_arrivals,
    safe_cast(overseas_departures as int64) overseas_departures,
    safe_cast(net_overseas_migration as int64) net_overseas_migration,
    safe_cast(area_sqkm as float64) area_sqkm,
    safe_cast(population_density as float64) population_density
from {{ set_datalake_project("au_abs_population_staging.regional_lga") }} as t
