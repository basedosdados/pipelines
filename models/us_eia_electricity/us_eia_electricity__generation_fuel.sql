{{
    config(
        schema="us_eia_electricity",
        alias="generation_fuel",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2001, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(plant_id as string) plant_id,
    safe_cast(plant_name as string) plant_name,
    safe_cast(operator_id as string) operator_id,
    safe_cast(operator_name as string) operator_name,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(state_id as string) state_id,
    safe_cast(census_region as string) census_region,
    safe_cast(nerc_region as string) nerc_region,
    safe_cast(balancing_authority_code as string) balancing_authority_code,
    safe_cast(sector_id as string) sector_id,
    safe_cast(sector_name as string) sector_name,
    safe_cast(naics_code as string) naics_code,
    safe_cast(prime_mover_code as string) prime_mover_code,
    safe_cast(energy_source_code as string) energy_source_code,
    safe_cast(fuel_type_code_agg as string) fuel_type_code_agg,
    safe_cast(nuclear_unit_id as string) nuclear_unit_id,
    safe_cast(associated_combined_heat_power as string) associated_combined_heat_power,
    safe_cast(reporting_frequency_code as string) reporting_frequency_code,
    safe_cast(fuel_unit as string) fuel_unit,
    safe_cast(fuel_consumed_units as float64) fuel_consumed_units,
    safe_cast(
        fuel_consumed_for_electricity_units as float64
    ) fuel_consumed_for_electricity_units,
    safe_cast(fuel_mmbtu_per_unit as float64) fuel_mmbtu_per_unit,
    safe_cast(fuel_consumed_mmbtu as float64) fuel_consumed_mmbtu,
    safe_cast(
        fuel_consumed_for_electricity_mmbtu as float64
    ) fuel_consumed_for_electricity_mmbtu,
    safe_cast(net_generation_mwh as float64) net_generation_mwh,
    safe_cast(data_maturity as string) data_maturity
from {{ set_datalake_project("us_eia_electricity_staging.generation_fuel") }} as t
