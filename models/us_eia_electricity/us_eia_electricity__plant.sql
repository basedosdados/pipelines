{{
    config(
        schema="us_eia_electricity",
        alias="plant",
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
    safe_cast(plant_id as string) plant_id,
    safe_cast(plant_name as string) plant_name,
    safe_cast(utility_id as string) utility_id,
    safe_cast(utility_name as string) utility_name,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(state_id as string) state_id,
    safe_cast(county_name as string) county_name,
    safe_cast(county_id as string) county_id,
    safe_cast(city as string) city,
    safe_cast(street_address as string) street_address,
    safe_cast(zip_code as string) zip_code,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(balancing_authority_code as string) balancing_authority_code,
    safe_cast(balancing_authority_name as string) balancing_authority_name,
    safe_cast(nerc_region as string) nerc_region,
    safe_cast(iso_rto_code as string) iso_rto_code,
    safe_cast(sector_id as string) sector_id,
    safe_cast(sector_name as string) sector_name,
    safe_cast(primary_purpose_naics_code as string) primary_purpose_naics_code,
    safe_cast(regulatory_status_code as string) regulatory_status_code,
    safe_cast(service_area as string) service_area,
    safe_cast(water_source as string) water_source,
    safe_cast(grid_voltage_1_kv as float64) grid_voltage_1_kv,
    safe_cast(grid_voltage_2_kv as float64) grid_voltage_2_kv,
    safe_cast(grid_voltage_3_kv as float64) grid_voltage_3_kv,
    safe_cast(ferc_cogen_status as string) ferc_cogen_status,
    safe_cast(ferc_small_power_producer as string) ferc_small_power_producer,
    safe_cast(
        ferc_exempt_wholesale_generator as string
    ) ferc_exempt_wholesale_generator,
    safe_cast(
        transmission_distribution_owner_id as string
    ) transmission_distribution_owner_id,
    safe_cast(
        transmission_distribution_owner_name as string
    ) transmission_distribution_owner_name,
    safe_cast(
        transmission_distribution_owner_state as string
    ) transmission_distribution_owner_state,
    safe_cast(natural_gas_pipeline_name_1 as string) natural_gas_pipeline_name_1,
    safe_cast(natural_gas_pipeline_name_2 as string) natural_gas_pipeline_name_2,
    safe_cast(natural_gas_pipeline_name_3 as string) natural_gas_pipeline_name_3,
    safe_cast(
        natural_gas_local_distribution_company as string
    ) natural_gas_local_distribution_company,
    safe_cast(natural_gas_storage as string) natural_gas_storage,
    safe_cast(liquefied_natural_gas_storage as string) liquefied_natural_gas_storage,
    safe_cast(ash_impoundment as string) ash_impoundment,
    safe_cast(ash_impoundment_lined as string) ash_impoundment_lined,
    safe_cast(ash_impoundment_status as string) ash_impoundment_status,
    safe_cast(energy_storage as string) energy_storage,
    safe_cast(has_net_metering as string) has_net_metering,
    safe_cast(datum as string) datum,
    safe_cast(data_maturity as string) data_maturity
from {{ set_datalake_project("us_eia_electricity_staging.plant") }} as t
