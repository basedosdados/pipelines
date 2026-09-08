{{
    config(
        schema="us_eia_electricity",
        alias="fuel_receipts_costs",
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
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(state_id as string) state_id,
    safe_cast(operator_id as string) operator_id,
    safe_cast(operator_name as string) operator_name,
    safe_cast(balancing_authority_code as string) balancing_authority_code,
    safe_cast(energy_source_code as string) energy_source_code,
    safe_cast(fuel_group_code as string) fuel_group_code,
    safe_cast(contract_type_code as string) contract_type_code,
    safe_cast(contract_expiration_date as date) contract_expiration_date,
    safe_cast(supplier_name as string) supplier_name,
    safe_cast(mine_name as string) mine_name,
    safe_cast(mine_id_msha as string) mine_id_msha,
    safe_cast(mine_type_code as string) mine_type_code,
    safe_cast(mine_state_abbreviation as string) mine_state_abbreviation,
    safe_cast(mine_county_id as string) mine_county_id,
    safe_cast(
        primary_transportation_mode_code as string
    ) primary_transportation_mode_code,
    safe_cast(
        secondary_transportation_mode_code as string
    ) secondary_transportation_mode_code,
    safe_cast(natural_gas_transport_code as string) natural_gas_transport_code,
    safe_cast(
        natural_gas_delivery_contract_type_code as string
    ) natural_gas_delivery_contract_type_code,
    safe_cast(fuel_received_units as float64) fuel_received_units,
    safe_cast(fuel_mmbtu_per_unit as float64) fuel_mmbtu_per_unit,
    safe_cast(fuel_cost_per_mmbtu as float64) fuel_cost_per_mmbtu,
    safe_cast(sulfur_content_pct as float64) sulfur_content_pct,
    safe_cast(ash_content_pct as float64) ash_content_pct,
    safe_cast(moisture_content_pct as float64) moisture_content_pct,
    safe_cast(mercury_content_ppm as float64) mercury_content_ppm,
    safe_cast(chlorine_content_ppm as float64) chlorine_content_ppm,
    safe_cast(regulated as string) regulated,
    safe_cast(reporting_frequency_code as string) reporting_frequency_code,
    safe_cast(data_maturity as string) data_maturity
from {{ set_datalake_project("us_eia_electricity_staging.fuel_receipts_costs") }} as t
