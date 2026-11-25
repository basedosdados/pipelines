{{
    config(
        alias="nfip_policy",
        schema="us_fema_openfema",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(county_id as string) county_id,
    safe_cast(census_tract_id as string) census_tract_id,
    safe_cast(census_block_group_id as string) census_block_group_id,
    safe_cast(policy_id as string) policy_id,
    safe_cast(policy_effective_date as date) policy_effective_date,
    safe_cast(
        agriculture_structure_indicator as boolean
    ) agriculture_structure_indicator,
    safe_cast(as_of_date as datetime) as_of_date,
    safe_cast(base_flood_elevation as float64) base_flood_elevation,
    safe_cast(
        basement_enclosure_crawlspace_type as string
    ) basement_enclosure_crawlspace_type,
    safe_cast(
        cancellation_date_of_flood_policy as date
    ) cancellation_date_of_flood_policy,
    safe_cast(condominium_coverage_type_code as string) condominium_coverage_type_code,
    safe_cast(construction as boolean) construction,
    safe_cast(crs_class_code as string) crs_class_code,
    safe_cast(building_deductible_code as string) building_deductible_code,
    safe_cast(contents_deductible_code as string) contents_deductible_code,
    safe_cast(elevated_building_indicator as boolean) elevated_building_indicator,
    safe_cast(
        elevation_certificate_indicator as string
    ) elevation_certificate_indicator,
    safe_cast(elevation_difference as int64) elevation_difference,
    safe_cast(federal_policy_fee as int64) federal_policy_fee,
    safe_cast(rated_flood_zone as string) rated_flood_zone,
    safe_cast(hfiaa_surcharge as int64) hfiaa_surcharge,
    safe_cast(house_of_worship_indicator as boolean) house_of_worship_indicator,
    safe_cast(location_of_contents as string) location_of_contents,
    safe_cast(lowest_adjacent_grade as float64) lowest_adjacent_grade,
    safe_cast(lowest_floor_elevation as float64) lowest_floor_elevation,
    safe_cast(non_profit_indicator as boolean) non_profit_indicator,
    safe_cast(
        number_of_floors_in_insured_building as string
    ) number_of_floors_in_insured_building,
    safe_cast(obstruction_type as string) obstruction_type,
    safe_cast(occupancy_type as string) occupancy_type,
    safe_cast(original_construction_date as date) original_construction_date,
    safe_cast(original_nb_date as date) original_nb_date,
    safe_cast(policy_cost as int64) policy_cost,
    safe_cast(policy_count as int64) policy_count,
    safe_cast(policy_termination_date as date) policy_termination_date,
    safe_cast(policy_term_indicator as string) policy_term_indicator,
    safe_cast(
        post_firm_construction_indicator as boolean
    ) post_firm_construction_indicator,
    safe_cast(primary_residence_indicator as boolean) primary_residence_indicator,
    safe_cast(rate_method as string) rate_method,
    safe_cast(
        regular_emergency_program_indicator as string
    ) regular_emergency_program_indicator,
    safe_cast(
        small_business_indicator_building as boolean
    ) small_business_indicator_building,
    safe_cast(
        total_building_insurance_coverage as int64
    ) total_building_insurance_coverage,
    safe_cast(
        total_contents_insurance_coverage as int64
    ) total_contents_insurance_coverage,
    safe_cast(
        total_insurance_premium_of_the_policy as int64
    ) total_insurance_premium_of_the_policy,
    safe_cast(
        cancellation_voidance_reason_code as string
    ) cancellation_voidance_reason_code,
    safe_cast(subsidized_rate_type as string) subsidized_rate_type,
    safe_cast(icc_premium as int64) icc_premium,
    safe_cast(reserve_fund_assessment as int64) reserve_fund_assessment,
    safe_cast(community_probation_surcharge as int64) community_probation_surcharge,
    safe_cast(premium_payment_indicator as string) premium_payment_indicator,
    safe_cast(building_replacement_cost as int64) building_replacement_cost,
    safe_cast(basic_building_rate as float64) basic_building_rate,
    safe_cast(additional_building_rate as float64) additional_building_rate,
    safe_cast(basic_contents_rate as float64) basic_contents_rate,
    safe_cast(additional_contents_rate as float64) additional_contents_rate,
    safe_cast(enclosure_type_code as string) enclosure_type_code,
    safe_cast(building_description_code as string) building_description_code,
    safe_cast(insurance_to_value_code as string) insurance_to_value_code,
    safe_cast(post_firm_vzone_indicator as boolean) post_firm_vzone_indicator,
    safe_cast(floodproofed_indicator as boolean) floodproofed_indicator,
    safe_cast(waiting_period_type as string) waiting_period_type,
    safe_cast(rollover_transfer_code as string) rollover_transfer_code,
    safe_cast(endorsement_effective_date as date) endorsement_effective_date,
    safe_cast(property_purchase_date as date) property_purchase_date,
    safe_cast(rental_property_indicator as boolean) rental_property_indicator,
    safe_cast(tenant_indicator as boolean) tenant_indicator,
    safe_cast(state_owned_indicator as boolean) state_owned_indicator,
    safe_cast(
        disaster_assistance_coverage_required as string
    ) disaster_assistance_coverage_required,
    safe_cast(mandatory_purchase_flag as boolean) mandatory_purchase_flag,
    safe_cast(grandfathering_type_code as string) grandfathering_type_code,
    safe_cast(nfip_rated_community_number as string) nfip_rated_community_number,
    safe_cast(nfip_community_number_current as string) nfip_community_number_current,
    safe_cast(nfip_community_name as string) nfip_community_name,
    safe_cast(program_type_indicator as boolean) program_type_indicator,
    safe_cast(map_panel_number as string) map_panel_number,
    safe_cast(map_panel_suffix as string) map_panel_suffix,
    safe_cast(flood_zone_current as string) flood_zone_current,
    safe_cast(fema_region as int64) fema_region,
    safe_cast(reported_city as string) reported_city,
    safe_cast(reported_zip_code as string) reported_zip_code,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(building_on_federal_land as boolean) building_on_federal_land,
    safe_cast(building_purpose as string) building_purpose,
    safe_cast(seasonally_occupied as boolean) seasonally_occupied,
    safe_cast(full_risk_premium as int64) full_risk_premium,
    safe_cast(building_over_water_type as string) building_over_water_type,
    safe_cast(foundation_type as string) foundation_type,
    safe_cast(
        pre_firm_construction_indicator as boolean
    ) pre_firm_construction_indicator
from {{ set_datalake_project("us_fema_openfema_staging.nfip_policy") }} as t
