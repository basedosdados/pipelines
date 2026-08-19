{{
    config(
        schema="us_treasury_usaspending",
        alias="contract_transaction",
        materialized="table",
        partition_by={
            "field": "fiscal_year",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2031, "interval": 1},
        },
        cluster_by=["awarding_agency_code", "recipient_uei", "recipient_state_code"],
    )
}}


select
    safe_cast(fiscal_year as int64) fiscal_year,
    safe_cast(
        contract_transaction_unique_key as string
    ) contract_transaction_unique_key,
    safe_cast(contract_award_unique_key as string) contract_award_unique_key,
    safe_cast(award_id_piid as string) award_id_piid,
    safe_cast(modification_number as string) modification_number,
    safe_cast(transaction_number as string) transaction_number,
    safe_cast(parent_award_agency_id as string) parent_award_agency_id,
    safe_cast(parent_award_agency_name as string) parent_award_agency_name,
    safe_cast(parent_award_id_piid as string) parent_award_id_piid,
    safe_cast(
        parent_award_modification_number as string
    ) parent_award_modification_number,
    safe_cast(federal_action_obligation as float64) federal_action_obligation,
    safe_cast(total_dollars_obligated as float64) total_dollars_obligated,
    safe_cast(
        total_outlayed_amount_for_overall_award as float64
    ) total_outlayed_amount_for_overall_award,
    safe_cast(
        base_and_exercised_options_value as float64
    ) base_and_exercised_options_value,
    safe_cast(current_total_value_of_award as float64) current_total_value_of_award,
    safe_cast(base_and_all_options_value as float64) base_and_all_options_value,
    safe_cast(potential_total_value_of_award as float64) potential_total_value_of_award,
    safe_cast(
        disaster_emergency_fund_codes_for_overall_award as string
    ) disaster_emergency_fund_codes_for_overall_award,
    safe_cast(
        outlayed_amount_from_covid19_supplementals_for_overall_award as float64
    ) outlayed_amount_from_covid19_supplementals_for_overall_award,
    safe_cast(
        obligated_amount_from_covid19_supplementals_for_overall_award as float64
    ) obligated_amount_from_covid19_supplementals_for_overall_award,
    safe_cast(
        outlayed_amount_from_iija_supplemental_for_overall_award as float64
    ) outlayed_amount_from_iija_supplemental_for_overall_award,
    safe_cast(
        obligated_amount_from_iija_supplemental_for_overall_award as float64
    ) obligated_amount_from_iija_supplemental_for_overall_award,
    safe_cast(action_date as date) action_date,
    safe_cast(
        period_of_performance_start_date as date
    ) period_of_performance_start_date,
    safe_cast(
        period_of_performance_current_end_date as date
    ) period_of_performance_current_end_date,
    safe_cast(
        period_of_performance_potential_end_date as date
    ) period_of_performance_potential_end_date,
    safe_cast(ordering_period_end_date as date) ordering_period_end_date,
    safe_cast(solicitation_date as date) solicitation_date,
    safe_cast(awarding_agency_code as string) awarding_agency_code,
    safe_cast(awarding_agency_name as string) awarding_agency_name,
    safe_cast(awarding_sub_agency_code as string) awarding_sub_agency_code,
    safe_cast(awarding_sub_agency_name as string) awarding_sub_agency_name,
    safe_cast(awarding_office_code as string) awarding_office_code,
    safe_cast(awarding_office_name as string) awarding_office_name,
    safe_cast(funding_agency_code as string) funding_agency_code,
    safe_cast(funding_agency_name as string) funding_agency_name,
    safe_cast(funding_sub_agency_code as string) funding_sub_agency_code,
    safe_cast(funding_sub_agency_name as string) funding_sub_agency_name,
    safe_cast(funding_office_code as string) funding_office_code,
    safe_cast(funding_office_name as string) funding_office_name,
    safe_cast(
        treasury_accounts_funding_this_award as string
    ) treasury_accounts_funding_this_award,
    safe_cast(
        federal_accounts_funding_this_award as string
    ) federal_accounts_funding_this_award,
    safe_cast(
        object_classes_funding_this_award as string
    ) object_classes_funding_this_award,
    safe_cast(
        program_activities_funding_this_award as string
    ) program_activities_funding_this_award,
    safe_cast(foreign_funding as string) foreign_funding,
    safe_cast(foreign_funding_description as string) foreign_funding_description,
    safe_cast(sam_exception as string) sam_exception,
    safe_cast(sam_exception_description as string) sam_exception_description,
    safe_cast(recipient_uei as string) recipient_uei,
    safe_cast(recipient_duns as string) recipient_duns,
    safe_cast(recipient_name as string) recipient_name,
    safe_cast(recipient_name_raw as string) recipient_name_raw,
    safe_cast(
        recipient_doing_business_as_name as string
    ) recipient_doing_business_as_name,
    safe_cast(cage_code as string) cage_code,
    safe_cast(recipient_parent_uei as string) recipient_parent_uei,
    safe_cast(recipient_parent_duns as string) recipient_parent_duns,
    safe_cast(recipient_parent_name as string) recipient_parent_name,
    safe_cast(recipient_parent_name_raw as string) recipient_parent_name_raw,
    safe_cast(recipient_country_code as string) recipient_country_code,
    safe_cast(recipient_country_name as string) recipient_country_name,
    safe_cast(recipient_address_line_1 as string) recipient_address_line_1,
    safe_cast(recipient_address_line_2 as string) recipient_address_line_2,
    safe_cast(recipient_city_name as string) recipient_city_name,
    safe_cast(
        concat(
            substr(
                regexp_replace(
                    prime_award_transaction_recipient_county_fips_code, r'\.0$', ''
                ),
                1,
                2
            ),
            lpad(
                substr(
                    regexp_replace(
                        prime_award_transaction_recipient_county_fips_code, r'\.0$', ''
                    ),
                    3
                ),
                3,
                '0'
            )
        ) as string
    ) prime_award_transaction_recipient_county_fips_code,
    safe_cast(recipient_county_name as string) recipient_county_name,
    safe_cast(
        prime_award_transaction_recipient_state_fips_code as string
    ) prime_award_transaction_recipient_state_fips_code,
    safe_cast(recipient_state_code as string) recipient_state_code,
    safe_cast(recipient_state_name as string) recipient_state_name,
    safe_cast(recipient_zip_4_code as string) recipient_zip_4_code,
    safe_cast(
        prime_award_transaction_recipient_cd_original as string
    ) prime_award_transaction_recipient_cd_original,
    safe_cast(
        prime_award_transaction_recipient_cd_current as string
    ) prime_award_transaction_recipient_cd_current,
    safe_cast(recipient_phone_number as string) recipient_phone_number,
    safe_cast(recipient_fax_number as string) recipient_fax_number,
    safe_cast(
        primary_place_of_performance_country_code as string
    ) primary_place_of_performance_country_code,
    safe_cast(
        primary_place_of_performance_country_name as string
    ) primary_place_of_performance_country_name,
    safe_cast(
        primary_place_of_performance_city_name as string
    ) primary_place_of_performance_city_name,
    safe_cast(
        concat(
            substr(
                regexp_replace(
                    prime_award_transaction_place_of_performance_county_fips_code,
                    r'\.0$',
                    ''
                ),
                1,
                2
            ),
            lpad(
                substr(
                    regexp_replace(
                        prime_award_transaction_place_of_performance_county_fips_code,
                        r'\.0$',
                        ''
                    ),
                    3
                ),
                3,
                '0'
            )
        ) as string
    ) prime_award_transaction_place_of_performance_county_fips_code,
    safe_cast(
        primary_place_of_performance_county_name as string
    ) primary_place_of_performance_county_name,
    safe_cast(
        prime_award_transaction_place_of_performance_state_fips_code as string
    ) prime_award_transaction_place_of_performance_state_fips_code,
    safe_cast(
        primary_place_of_performance_state_code as string
    ) primary_place_of_performance_state_code,
    safe_cast(
        primary_place_of_performance_state_name as string
    ) primary_place_of_performance_state_name,
    safe_cast(
        primary_place_of_performance_zip_4 as string
    ) primary_place_of_performance_zip_4,
    safe_cast(
        prime_award_transaction_place_of_performance_cd_original as string
    ) prime_award_transaction_place_of_performance_cd_original,
    safe_cast(
        prime_award_transaction_place_of_performance_cd_current as string
    ) prime_award_transaction_place_of_performance_cd_current,
    safe_cast(award_or_idv_flag as string) award_or_idv_flag,
    safe_cast(award_type_code as string) award_type_code,
    safe_cast(award_type as string) award_type,
    safe_cast(idv_type_code as string) idv_type_code,
    safe_cast(idv_type as string) idv_type,
    safe_cast(
        multiple_or_single_award_idv_code as string
    ) multiple_or_single_award_idv_code,
    safe_cast(multiple_or_single_award_idv as string) multiple_or_single_award_idv,
    safe_cast(type_of_idc_code as string) type_of_idc_code,
    safe_cast(type_of_idc as string) type_of_idc,
    safe_cast(type_of_contract_pricing_code as string) type_of_contract_pricing_code,
    safe_cast(type_of_contract_pricing as string) type_of_contract_pricing,
    safe_cast(transaction_description as string) transaction_description,
    safe_cast(
        prime_award_base_transaction_description as string
    ) prime_award_base_transaction_description,
    safe_cast(action_type_code as string) action_type_code,
    safe_cast(action_type as string) action_type,
    safe_cast(solicitation_identifier as string) solicitation_identifier,
    safe_cast(number_of_actions as int64) number_of_actions,
    safe_cast(
        inherently_governmental_functions as string
    ) inherently_governmental_functions,
    safe_cast(
        inherently_governmental_functions_description as string
    ) inherently_governmental_functions_description,
    safe_cast(product_or_service_code as string) product_or_service_code,
    safe_cast(
        product_or_service_code_description as string
    ) product_or_service_code_description,
    safe_cast(contract_bundling_code as string) contract_bundling_code,
    safe_cast(contract_bundling as string) contract_bundling,
    safe_cast(dod_claimant_program_code as string) dod_claimant_program_code,
    safe_cast(
        dod_claimant_program_description as string
    ) dod_claimant_program_description,
    safe_cast(naics_code as string) naics_code,
    safe_cast(naics_description as string) naics_description,
    safe_cast(
        recovered_materials_sustainability_code as string
    ) recovered_materials_sustainability_code,
    safe_cast(
        recovered_materials_sustainability as string
    ) recovered_materials_sustainability,
    safe_cast(
        domestic_or_foreign_entity_code as string
    ) domestic_or_foreign_entity_code,
    safe_cast(domestic_or_foreign_entity as string) domestic_or_foreign_entity,
    safe_cast(dod_acquisition_program_code as string) dod_acquisition_program_code,
    safe_cast(
        dod_acquisition_program_description as string
    ) dod_acquisition_program_description,
    safe_cast(
        information_technology_commercial_item_category_code as string
    ) information_technology_commercial_item_category_code,
    safe_cast(
        information_technology_commercial_item_category as string
    ) information_technology_commercial_item_category,
    safe_cast(epa_designated_product_code as string) epa_designated_product_code,
    safe_cast(epa_designated_product as string) epa_designated_product,
    safe_cast(
        country_of_product_or_service_origin_code as string
    ) country_of_product_or_service_origin_code,
    safe_cast(
        country_of_product_or_service_origin as string
    ) country_of_product_or_service_origin,
    safe_cast(place_of_manufacture_code as string) place_of_manufacture_code,
    safe_cast(place_of_manufacture as string) place_of_manufacture,
    safe_cast(subcontracting_plan_code as string) subcontracting_plan_code,
    safe_cast(subcontracting_plan as string) subcontracting_plan,
    safe_cast(extent_competed_code as string) extent_competed_code,
    safe_cast(extent_competed as string) extent_competed,
    safe_cast(solicitation_procedures_code as string) solicitation_procedures_code,
    safe_cast(solicitation_procedures as string) solicitation_procedures,
    safe_cast(type_of_set_aside_code as string) type_of_set_aside_code,
    safe_cast(type_of_set_aside as string) type_of_set_aside,
    safe_cast(evaluated_preference_code as string) evaluated_preference_code,
    safe_cast(evaluated_preference as string) evaluated_preference,
    safe_cast(research_code as string) research_code,
    safe_cast(research as string) research,
    safe_cast(
        fair_opportunity_limited_sources_code as string
    ) fair_opportunity_limited_sources_code,
    safe_cast(
        fair_opportunity_limited_sources as string
    ) fair_opportunity_limited_sources,
    safe_cast(
        other_than_full_and_open_competition_code as string
    ) other_than_full_and_open_competition_code,
    safe_cast(
        other_than_full_and_open_competition as string
    ) other_than_full_and_open_competition,
    safe_cast(number_of_offers_received as int64) number_of_offers_received,
    safe_cast(
        commercial_item_acquisition_procedures_code as string
    ) commercial_item_acquisition_procedures_code,
    safe_cast(
        commercial_item_acquisition_procedures as string
    ) commercial_item_acquisition_procedures,
    safe_cast(
        small_business_competitiveness_demonstration_program as string
    ) small_business_competitiveness_demonstration_program,
    safe_cast(
        simplified_procedures_for_certain_commercial_items_code as string
    ) simplified_procedures_for_certain_commercial_items_code,
    safe_cast(
        simplified_procedures_for_certain_commercial_items as string
    ) simplified_procedures_for_certain_commercial_items,
    safe_cast(a76_fair_act_action_code as string) a76_fair_act_action_code,
    safe_cast(a76_fair_act_action as string) a76_fair_act_action,
    safe_cast(fed_biz_opps_code as string) fed_biz_opps_code,
    safe_cast(fed_biz_opps as string) fed_biz_opps,
    safe_cast(local_area_set_aside_code as string) local_area_set_aside_code,
    safe_cast(local_area_set_aside as string) local_area_set_aside,
    safe_cast(
        price_evaluation_adjustment_preference_percent_difference as float64
    ) price_evaluation_adjustment_preference_percent_difference,
    safe_cast(
        clinger_cohen_act_planning_code as string
    ) clinger_cohen_act_planning_code,
    safe_cast(clinger_cohen_act_planning as string) clinger_cohen_act_planning,
    safe_cast(
        materials_supplies_articles_equipment_code as string
    ) materials_supplies_articles_equipment_code,
    safe_cast(
        materials_supplies_articles_equipment as string
    ) materials_supplies_articles_equipment,
    safe_cast(labor_standards_code as string) labor_standards_code,
    safe_cast(labor_standards as string) labor_standards,
    safe_cast(
        construction_wage_rate_requirements_code as string
    ) construction_wage_rate_requirements_code,
    safe_cast(
        construction_wage_rate_requirements as string
    ) construction_wage_rate_requirements,
    safe_cast(
        interagency_contracting_authority_code as string
    ) interagency_contracting_authority_code,
    safe_cast(
        interagency_contracting_authority as string
    ) interagency_contracting_authority,
    safe_cast(other_statutory_authority as string) other_statutory_authority,
    safe_cast(program_acronym as string) program_acronym,
    safe_cast(parent_award_type_code as string) parent_award_type_code,
    safe_cast(parent_award_type as string) parent_award_type,
    safe_cast(
        parent_award_single_or_multiple_code as string
    ) parent_award_single_or_multiple_code,
    safe_cast(
        parent_award_single_or_multiple as string
    ) parent_award_single_or_multiple,
    safe_cast(major_program as string) major_program,
    safe_cast(national_interest_action_code as string) national_interest_action_code,
    safe_cast(national_interest_action as string) national_interest_action,
    safe_cast(cost_or_pricing_data_code as string) cost_or_pricing_data_code,
    safe_cast(cost_or_pricing_data as string) cost_or_pricing_data,
    safe_cast(
        cost_accounting_standards_clause_code as string
    ) cost_accounting_standards_clause_code,
    safe_cast(
        cost_accounting_standards_clause as string
    ) cost_accounting_standards_clause,
    safe_cast(
        government_furnished_property_code as string
    ) government_furnished_property_code,
    safe_cast(government_furnished_property as string) government_furnished_property,
    safe_cast(sea_transportation_code as string) sea_transportation_code,
    safe_cast(sea_transportation as string) sea_transportation,
    safe_cast(undefinitized_action_code as string) undefinitized_action_code,
    safe_cast(undefinitized_action as string) undefinitized_action,
    safe_cast(consolidated_contract_code as string) consolidated_contract_code,
    safe_cast(consolidated_contract as string) consolidated_contract,
    safe_cast(
        performance_based_service_acquisition_code as string
    ) performance_based_service_acquisition_code,
    safe_cast(
        performance_based_service_acquisition as string
    ) performance_based_service_acquisition,
    safe_cast(multi_year_contract_code as string) multi_year_contract_code,
    safe_cast(multi_year_contract as string) multi_year_contract,
    safe_cast(contract_financing_code as string) contract_financing_code,
    safe_cast(contract_financing as string) contract_financing,
    safe_cast(
        purchase_card_as_payment_method_code as string
    ) purchase_card_as_payment_method_code,
    safe_cast(
        purchase_card_as_payment_method as string
    ) purchase_card_as_payment_method,
    safe_cast(
        contingency_humanitarian_or_peacekeeping_operation_code as string
    ) contingency_humanitarian_or_peacekeeping_operation_code,
    safe_cast(
        contingency_humanitarian_or_peacekeeping_operation as string
    ) contingency_humanitarian_or_peacekeeping_operation,
    safe_cast(
        alaskan_native_corporation_owned_firm as string
    ) alaskan_native_corporation_owned_firm,
    safe_cast(american_indian_owned_business as string) american_indian_owned_business,
    safe_cast(
        indian_tribe_federally_recognized as string
    ) indian_tribe_federally_recognized,
    safe_cast(
        native_hawaiian_organization_owned_firm as string
    ) native_hawaiian_organization_owned_firm,
    safe_cast(tribally_owned_firm as string) tribally_owned_firm,
    safe_cast(veteran_owned_business as string) veteran_owned_business,
    safe_cast(
        service_disabled_veteran_owned_business as string
    ) service_disabled_veteran_owned_business,
    safe_cast(woman_owned_business as string) woman_owned_business,
    safe_cast(women_owned_small_business as string) women_owned_small_business,
    safe_cast(
        economically_disadvantaged_women_owned_small_business as string
    ) economically_disadvantaged_women_owned_small_business,
    safe_cast(
        joint_venture_women_owned_small_business as string
    ) joint_venture_women_owned_small_business,
    safe_cast(
        joint_venture_economic_disadvantaged_women_owned_small_bus as string
    ) joint_venture_economic_disadvantaged_women_owned_small_bus,
    safe_cast(minority_owned_business as string) minority_owned_business,
    safe_cast(
        subcontinent_asian_asian_indian_american_owned_business as string
    ) subcontinent_asian_asian_indian_american_owned_business,
    safe_cast(
        asian_pacific_american_owned_business as string
    ) asian_pacific_american_owned_business,
    safe_cast(black_american_owned_business as string) black_american_owned_business,
    safe_cast(
        hispanic_american_owned_business as string
    ) hispanic_american_owned_business,
    safe_cast(native_american_owned_business as string) native_american_owned_business,
    safe_cast(other_minority_owned_business as string) other_minority_owned_business,
    safe_cast(
        contracting_officers_determination_of_business_size as string
    ) contracting_officers_determination_of_business_size,
    safe_cast(
        contracting_officers_determination_of_business_size_code as string
    ) contracting_officers_determination_of_business_size_code,
    safe_cast(emerging_small_business as string) emerging_small_business,
    safe_cast(
        community_developed_corporation_owned_firm as string
    ) community_developed_corporation_owned_firm,
    safe_cast(labor_surplus_area_firm as string) labor_surplus_area_firm,
    safe_cast(us_federal_government as string) us_federal_government,
    safe_cast(
        federally_funded_research_and_development_corp as string
    ) federally_funded_research_and_development_corp,
    safe_cast(federal_agency as string) federal_agency,
    safe_cast(us_state_government as string) us_state_government,
    safe_cast(us_local_government as string) us_local_government,
    safe_cast(city_local_government as string) city_local_government,
    safe_cast(county_local_government as string) county_local_government,
    safe_cast(
        inter_municipal_local_government as string
    ) inter_municipal_local_government,
    safe_cast(local_government_owned as string) local_government_owned,
    safe_cast(municipality_local_government as string) municipality_local_government,
    safe_cast(
        school_district_local_government as string
    ) school_district_local_government,
    safe_cast(township_local_government as string) township_local_government,
    safe_cast(us_tribal_government as string) us_tribal_government,
    safe_cast(foreign_government as string) foreign_government,
    safe_cast(organizational_type as string) organizational_type,
    safe_cast(
        corporate_entity_not_tax_exempt as string
    ) corporate_entity_not_tax_exempt,
    safe_cast(corporate_entity_tax_exempt as string) corporate_entity_tax_exempt,
    safe_cast(
        partnership_or_limited_liability_partnership as string
    ) partnership_or_limited_liability_partnership,
    safe_cast(sole_proprietorship as string) sole_proprietorship,
    safe_cast(small_agricultural_cooperative as string) small_agricultural_cooperative,
    safe_cast(international_organization as string) international_organization,
    safe_cast(us_government_entity as string) us_government_entity,
    safe_cast(
        community_development_corporation as string
    ) community_development_corporation,
    safe_cast(domestic_shelter as string) domestic_shelter,
    safe_cast(educational_institution as string) educational_institution,
    safe_cast(foundation as string) foundation,
    safe_cast(hospital_flag as string) hospital_flag,
    safe_cast(manufacturer_of_goods as string) manufacturer_of_goods,
    safe_cast(veterinary_hospital as string) veterinary_hospital,
    safe_cast(hispanic_servicing_institution as string) hispanic_servicing_institution,
    safe_cast(receives_contracts as string) receives_contracts,
    safe_cast(receives_financial_assistance as string) receives_financial_assistance,
    safe_cast(
        receives_contracts_and_financial_assistance as string
    ) receives_contracts_and_financial_assistance,
    safe_cast(airport_authority as string) airport_authority,
    safe_cast(council_of_governments as string) council_of_governments,
    safe_cast(
        housing_authorities_public_tribal as string
    ) housing_authorities_public_tribal,
    safe_cast(interstate_entity as string) interstate_entity,
    safe_cast(planning_commission as string) planning_commission,
    safe_cast(port_authority as string) port_authority,
    safe_cast(transit_authority as string) transit_authority,
    safe_cast(subchapter_scorporation as string) subchapter_scorporation,
    safe_cast(limited_liability_corporation as string) limited_liability_corporation,
    safe_cast(foreign_owned as string) foreign_owned,
    safe_cast(for_profit_organization as string) for_profit_organization,
    safe_cast(nonprofit_organization as string) nonprofit_organization,
    safe_cast(
        other_not_for_profit_organization as string
    ) other_not_for_profit_organization,
    safe_cast(the_ability_one_program as string) the_ability_one_program,
    safe_cast(private_university_or_college as string) private_university_or_college,
    safe_cast(
        state_controlled_institution_of_higher_learning as string
    ) state_controlled_institution_of_higher_learning,
    safe_cast(land_grant_college_1862 as string) land_grant_college_1862,
    safe_cast(land_grant_college_1890 as string) land_grant_college_1890,
    safe_cast(land_grant_college_1994 as string) land_grant_college_1994,
    safe_cast(minority_institution as string) minority_institution,
    safe_cast(historically_black_college as string) historically_black_college,
    safe_cast(tribal_college as string) tribal_college,
    safe_cast(
        alaskan_native_servicing_institution as string
    ) alaskan_native_servicing_institution,
    safe_cast(
        native_hawaiian_servicing_institution as string
    ) native_hawaiian_servicing_institution,
    safe_cast(school_of_forestry as string) school_of_forestry,
    safe_cast(veterinary_college as string) veterinary_college,
    safe_cast(dot_certified_disadvantage as string) dot_certified_disadvantage,
    safe_cast(
        self_certified_small_disadvantaged_business as string
    ) self_certified_small_disadvantaged_business,
    safe_cast(small_disadvantaged_business as string) small_disadvantaged_business,
    safe_cast(c8a_program_participant as string) c8a_program_participant,
    safe_cast(
        historically_underutilized_business_zone_hubzone_firm as string
    ) historically_underutilized_business_zone_hubzone_firm,
    safe_cast(sba_certified_8a_joint_venture as string) sba_certified_8a_joint_venture,
    safe_cast(
        highly_compensated_officer_1_name as string
    ) highly_compensated_officer_1_name,
    safe_cast(
        highly_compensated_officer_1_amount as float64
    ) highly_compensated_officer_1_amount,
    safe_cast(
        highly_compensated_officer_2_name as string
    ) highly_compensated_officer_2_name,
    safe_cast(
        highly_compensated_officer_2_amount as float64
    ) highly_compensated_officer_2_amount,
    safe_cast(
        highly_compensated_officer_3_name as string
    ) highly_compensated_officer_3_name,
    safe_cast(
        highly_compensated_officer_3_amount as float64
    ) highly_compensated_officer_3_amount,
    safe_cast(
        highly_compensated_officer_4_name as string
    ) highly_compensated_officer_4_name,
    safe_cast(
        highly_compensated_officer_4_amount as float64
    ) highly_compensated_officer_4_amount,
    safe_cast(
        highly_compensated_officer_5_name as string
    ) highly_compensated_officer_5_name,
    safe_cast(
        highly_compensated_officer_5_amount as float64
    ) highly_compensated_officer_5_amount,
    safe_cast(usaspending_permalink as string) usaspending_permalink,
    safe_cast(initial_report_date as timestamp) initial_report_date,
    safe_cast(last_modified_date as timestamp) last_modified_date
from
    {{ set_datalake_project("us_treasury_usaspending_staging.contract_transaction") }}
    as t
