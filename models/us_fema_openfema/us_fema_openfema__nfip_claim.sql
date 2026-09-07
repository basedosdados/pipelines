{{
    config(
        alias="nfip_claim",
        schema="us_fema_openfema",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1978, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(county_id as string) county_id,
    safe_cast(census_tract_id as string) census_tract_id,
    safe_cast(census_block_group_id as string) census_block_group_id,
    safe_cast(claim_id as string) claim_id,
    safe_cast(date_of_loss as date) date_of_loss,
    safe_cast(
        agriculture_structure_indicator as boolean
    ) agriculture_structure_indicator,
    safe_cast(as_of_date as datetime) as_of_date,
    safe_cast(
        basement_enclosure_crawlspace_type as string
    ) basement_enclosure_crawlspace_type,
    safe_cast(policy_count as int64) policy_count,
    safe_cast(crs_class_code as string) crs_class_code,
    safe_cast(elevated_building_indicator as boolean) elevated_building_indicator,
    safe_cast(
        elevation_certificate_indicator as string
    ) elevation_certificate_indicator,
    safe_cast(elevation_difference as int64) elevation_difference,
    safe_cast(base_flood_elevation as float64) base_flood_elevation,
    safe_cast(rated_flood_zone as string) rated_flood_zone,
    safe_cast(house_of_worship_indicator as boolean) house_of_worship_indicator,
    safe_cast(location_of_contents as string) location_of_contents,
    safe_cast(lowest_adjacent_grade as float64) lowest_adjacent_grade,
    safe_cast(lowest_floor_elevation as float64) lowest_floor_elevation,
    safe_cast(
        number_of_floors_in_insured_building as string
    ) number_of_floors_in_insured_building,
    safe_cast(non_profit_indicator as boolean) non_profit_indicator,
    safe_cast(obstruction_type as string) obstruction_type,
    safe_cast(occupancy_type as string) occupancy_type,
    safe_cast(original_construction_date as date) original_construction_date,
    safe_cast(original_nb_date as date) original_nb_date,
    safe_cast(amount_paid_on_building_claim as float64) amount_paid_on_building_claim,
    safe_cast(amount_paid_on_contents_claim as float64) amount_paid_on_contents_claim,
    safe_cast(
        amount_paid_on_increased_cost_of_compliance_claim as float64
    ) amount_paid_on_increased_cost_of_compliance_claim,
    safe_cast(
        post_firm_construction_indicator as boolean
    ) post_firm_construction_indicator,
    safe_cast(rate_method as string) rate_method,
    safe_cast(
        small_business_indicator_building as boolean
    ) small_business_indicator_building,
    safe_cast(
        total_building_insurance_coverage as int64
    ) total_building_insurance_coverage,
    safe_cast(
        total_contents_insurance_coverage as int64
    ) total_contents_insurance_coverage,
    safe_cast(primary_residence_indicator as boolean) primary_residence_indicator,
    safe_cast(building_damage_amount as int64) building_damage_amount,
    safe_cast(building_deductible_code as string) building_deductible_code,
    safe_cast(net_building_payment_amount as float64) net_building_payment_amount,
    safe_cast(building_property_value as int64) building_property_value,
    safe_cast(cause_of_damage as string) cause_of_damage,
    safe_cast(condominium_coverage_type_code as string) condominium_coverage_type_code,
    safe_cast(contents_damage_amount as int64) contents_damage_amount,
    safe_cast(contents_deductible_code as string) contents_deductible_code,
    safe_cast(net_contents_payment_amount as float64) net_contents_payment_amount,
    safe_cast(contents_property_value as int64) contents_property_value,
    safe_cast(
        disaster_assistance_coverage_required as string
    ) disaster_assistance_coverage_required,
    safe_cast(event_designation_number as string) event_designation_number,
    safe_cast(fico_number as string) fico_number,
    safe_cast(
        flood_characteristics_indicator as string
    ) flood_characteristics_indicator,
    safe_cast(flood_water_duration as int64) flood_water_duration,
    safe_cast(floodproofed_indicator as boolean) floodproofed_indicator,
    safe_cast(flood_event as string) flood_event,
    safe_cast(icc_coverage as int64) icc_coverage,
    safe_cast(net_icc_payment_amount as float64) net_icc_payment_amount,
    safe_cast(nfip_rated_community_number as string) nfip_rated_community_number,
    safe_cast(nfip_community_number_current as string) nfip_community_number_current,
    safe_cast(nfip_community_name as string) nfip_community_name,
    safe_cast(non_payment_reason_contents as string) non_payment_reason_contents,
    safe_cast(non_payment_reason_building as string) non_payment_reason_building,
    safe_cast(number_of_units as int64) number_of_units,
    safe_cast(building_replacement_cost as int64) building_replacement_cost,
    safe_cast(contents_replacement_cost as int64) contents_replacement_cost,
    safe_cast(replacement_cost_basis as string) replacement_cost_basis,
    safe_cast(state_owned_indicator as boolean) state_owned_indicator,
    safe_cast(water_depth as int64) water_depth,
    safe_cast(flood_zone_current as string) flood_zone_current,
    safe_cast(building_description_code as string) building_description_code,
    safe_cast(rental_property_indicator as boolean) rental_property_indicator,
    safe_cast(reported_city as string) reported_city,
    safe_cast(reported_zip_code as string) reported_zip_code,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(foundation_type as string) foundation_type,
    safe_cast(open_date as date) open_date,
    safe_cast(most_recent_recovery_date as date) most_recent_recovery_date,
    safe_cast(exterior_water_depth as int64) exterior_water_depth,
    safe_cast(interior_water_depth as int64) interior_water_depth,
    safe_cast(most_recent_payment_date as date) most_recent_payment_date,
    safe_cast(
        pre_firm_construction_indicator as boolean
    ) pre_firm_construction_indicator,
    safe_cast(total_salvage_recovery as float64) total_salvage_recovery,
    safe_cast(total_bldg_claim_pmt_recovery as float64) total_bldg_claim_pmt_recovery,
    safe_cast(
        total_contents_claim_pmt_recovery as float64
    ) total_contents_claim_pmt_recovery,
    safe_cast(total_icc_claim_pmt_recovery as float64) total_icc_claim_pmt_recovery,
    safe_cast(total_subrogation_recovery as float64) total_subrogation_recovery
from {{ set_datalake_project("us_fema_openfema_staging.nfip_claim") }} as t
