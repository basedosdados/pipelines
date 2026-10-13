{{
    config(
        schema="us_dot_fars",
        alias="vehicle",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1975, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_id as string) state_id,
    safe_cast(case_number as string) case_number,
    safe_cast(vehicle_number as int64) vehicle_number,
    safe_cast(unit_type_code as string) unit_type_code,
    safe_cast(occupants_count as int64) occupants_count,
    safe_cast(hit_and_run_code as string) hit_and_run_code,
    safe_cast(registration_state_code as string) registration_state_code,
    safe_cast(owner_code as string) owner_code,
    safe_cast(make_code as string) make_code,
    safe_cast(model_code as string) model_code,
    safe_cast(make_model_code as string) make_model_code,
    safe_cast(body_type_code as string) body_type_code,
    safe_cast(model_year as int64) model_year,
    safe_cast(vehicle_identification_number as string) vehicle_identification_number,
    safe_cast(tow_vehicle_code as string) tow_vehicle_code,
    safe_cast(jackknife_code as string) jackknife_code,
    safe_cast(
        gross_vehicle_weight_rating_code as string
    ) gross_vehicle_weight_rating_code,
    safe_cast(vehicle_configuration_code as string) vehicle_configuration_code,
    safe_cast(cargo_body_type_code as string) cargo_body_type_code,
    safe_cast(
        hazardous_material_involvement_code as string
    ) hazardous_material_involvement_code,
    safe_cast(bus_use_code as string) bus_use_code,
    safe_cast(special_use_code as string) special_use_code,
    safe_cast(emergency_use_code as string) emergency_use_code,
    safe_cast(travel_speed as int64) travel_speed,
    safe_cast(rollover_code as string) rollover_code,
    safe_cast(initial_impact_point_code as string) initial_impact_point_code,
    safe_cast(extent_of_damage_code as string) extent_of_damage_code,
    safe_cast(most_harmful_event_code as string) most_harmful_event_code,
    safe_cast(fire_occurrence_code as string) fire_occurrence_code,
    safe_cast(deaths_count as int64) deaths_count,
    safe_cast(driver_present_code as string) driver_present_code,
    safe_cast(driver_drinking_code as string) driver_drinking_code,
    safe_cast(driver_license_state_code as string) driver_license_state_code,
    safe_cast(driver_license_status_code as string) driver_license_status_code,
    safe_cast(driver_license_compliance_code as string) driver_license_compliance_code,
    safe_cast(commercial_license_status_code as string) commercial_license_status_code,
    safe_cast(driver_height as int64) driver_height,
    safe_cast(driver_weight as int64) driver_weight,
    safe_cast(previous_crashes_count as int64) previous_crashes_count,
    safe_cast(previous_suspensions_count as int64) previous_suspensions_count,
    safe_cast(previous_dwi_convictions_count as int64) previous_dwi_convictions_count,
    safe_cast(
        previous_speeding_convictions_count as int64
    ) previous_speeding_convictions_count,
    safe_cast(
        previous_other_convictions_count as int64
    ) previous_other_convictions_count,
    safe_cast(speeding_related_code as string) speeding_related_code,
    safe_cast(trafficway_description_code as string) trafficway_description_code,
    safe_cast(vehicle_number_of_lanes_code as string) vehicle_number_of_lanes_code,
    safe_cast(vehicle_speed_limit as int64) vehicle_speed_limit,
    safe_cast(vehicle_roadway_alignment_code as string) vehicle_roadway_alignment_code,
    safe_cast(vehicle_roadway_profile_code as string) vehicle_roadway_profile_code,
    safe_cast(vehicle_surface_condition_code as string) vehicle_surface_condition_code,
    safe_cast(vehicle_traffic_control_code as string) vehicle_traffic_control_code,
    safe_cast(
        vehicle_traffic_control_functioning_code as string
    ) vehicle_traffic_control_functioning_code,
    safe_cast(crash_type_code as string) crash_type_code,
    safe_cast(driver_maneuver_code as string) driver_maneuver_code,
    safe_cast(month as int64) month,
    safe_cast(day as int64) day,
    safe_cast(hour as int64) hour,
    safe_cast(minute as int64) minute,
    safe_cast(vehicle_forms_count as int64) vehicle_forms_count,
    safe_cast(first_harmful_event_code as string) first_harmful_event_code,
    safe_cast(manner_of_collision_code as string) manner_of_collision_code,
    safe_cast(most_damaged_area_code as string) most_damaged_area_code,
    safe_cast(vehicle_role_code as string) vehicle_role_code,
    safe_cast(vehicle_towed_code as string) vehicle_towed_code,
    safe_cast(rollover_location_code as string) rollover_location_code,
    safe_cast(underride_override_code as string) underride_override_code,
    safe_cast(
        vehicle_underride_override_code as string
    ) vehicle_underride_override_code,
    safe_cast(vehicle_maneuver_code as string) vehicle_maneuver_code,
    safe_cast(crash_avoidance_maneuver_code as string) crash_avoidance_maneuver_code,
    safe_cast(crash_type_configuration_code as string) crash_type_configuration_code,
    safe_cast(pre_event_movement_code as string) pre_event_movement_code,
    safe_cast(
        attempted_avoidance_maneuver_code as string
    ) attempted_avoidance_maneuver_code,
    safe_cast(pre_impact_stability_code as string) pre_impact_stability_code,
    safe_cast(pre_impact_location_code as string) pre_impact_location_code,
    safe_cast(
        vehicle_roadway_surface_type_code as string
    ) vehicle_roadway_surface_type_code,
    safe_cast(driver_vision_obscured_1_code as string) driver_vision_obscured_1_code,
    safe_cast(driver_vision_obscured_2_code as string) driver_vision_obscured_2_code,
    safe_cast(driver_vision_obscured_3_code as string) driver_vision_obscured_3_code,
    safe_cast(driver_training_code as string) driver_training_code,
    safe_cast(driver_zip_code as string) driver_zip_code,
    safe_cast(license_type_code as string) license_type_code,
    safe_cast(
        license_endorsement_compliance_code as string
    ) license_endorsement_compliance_code,
    safe_cast(
        license_restriction_compliance_code as string
    ) license_restriction_compliance_code,
    safe_cast(
        license_vehicle_class_compliance_code as string
    ) license_vehicle_class_compliance_code,
    safe_cast(
        previous_suspensions_bac_count_code as string
    ) previous_suspensions_bac_count_code,
    safe_cast(
        previous_other_suspensions_code as string
    ) previous_other_suspensions_code,
    safe_cast(first_record_month as int64) first_record_month,
    safe_cast(first_record_year as int64) first_record_year,
    safe_cast(last_record_month as int64) last_record_month,
    safe_cast(last_record_year as int64) last_record_year,
    safe_cast(violation_charged_1_code as string) violation_charged_1_code,
    safe_cast(violation_charged_2_code as string) violation_charged_2_code,
    safe_cast(violation_charged_3_code as string) violation_charged_3_code,
    safe_cast(driver_related_factor_1_code as string) driver_related_factor_1_code,
    safe_cast(driver_related_factor_2_code as string) driver_related_factor_2_code,
    safe_cast(driver_related_factor_3_code as string) driver_related_factor_3_code,
    safe_cast(driver_related_factor_4_code as string) driver_related_factor_4_code,
    safe_cast(vehicle_related_factor_1_code as string) vehicle_related_factor_1_code,
    safe_cast(vehicle_related_factor_2_code as string) vehicle_related_factor_2_code,
    safe_cast(sequence_of_events_1_code as string) sequence_of_events_1_code,
    safe_cast(sequence_of_events_2_code as string) sequence_of_events_2_code,
    safe_cast(sequence_of_events_3_code as string) sequence_of_events_3_code,
    safe_cast(sequence_of_events_4_code as string) sequence_of_events_4_code,
    safe_cast(sequence_of_events_5_code as string) sequence_of_events_5_code,
    safe_cast(sequence_of_events_6_code as string) sequence_of_events_6_code,
    safe_cast(axles_code as string) axles_code,
    safe_cast(
        hazardous_material_placard_code as string
    ) hazardous_material_placard_code,
    safe_cast(hazardous_material_id as string) hazardous_material_id,
    safe_cast(
        hazardous_material_class_number as string
    ) hazardous_material_class_number,
    safe_cast(
        hazardous_material_release_code as string
    ) hazardous_material_release_code,
    safe_cast(motor_carrier_id as string) motor_carrier_id,
    safe_cast(
        motor_carrier_id_issuing_authority_code as string
    ) motor_carrier_id_issuing_authority_code,
    safe_cast(motor_carrier_id_number as string) motor_carrier_id_number,
    safe_cast(trailer_1_vin as string) trailer_1_vin,
    safe_cast(trailer_2_vin as string) trailer_2_vin,
    safe_cast(trailer_3_vin as string) trailer_3_vin,
    safe_cast(
        trailer_1_gross_weight_rating_code as string
    ) trailer_1_gross_weight_rating_code,
    safe_cast(
        trailer_2_gross_weight_rating_code as string
    ) trailer_2_gross_weight_rating_code,
    safe_cast(
        trailer_3_gross_weight_rating_code as string
    ) trailer_3_gross_weight_rating_code,
    safe_cast(
        gross_vehicle_weight_rating_from_code as string
    ) gross_vehicle_weight_rating_from_code,
    safe_cast(
        gross_vehicle_weight_rating_to_code as string
    ) gross_vehicle_weight_rating_to_code,
    safe_cast(final_stage_body_class_code as string) final_stage_body_class_code,
    safe_cast(vpic_make_code as string) vpic_make_code,
    safe_cast(vpic_model_code as string) vpic_model_code,
    safe_cast(vpic_body_class_code as string) vpic_body_class_code,
    safe_cast(motorcycle_type_code as string) motorcycle_type_code,
    safe_cast(truck_chassis_type_code as string) truck_chassis_type_code,
    safe_cast(truck_field_code as string) truck_field_code,
    safe_cast(vin_make as string) vin_make,
    safe_cast(vin_model as string) vin_model,
    safe_cast(vin_body_type as string) vin_body_type,
    safe_cast(vin_vehicle_type as string) vin_vehicle_type,
    safe_cast(vin_model_year_code as string) vin_model_year_code,
    safe_cast(vin_length as int64) vin_length,
    safe_cast(vin_curb_weight as int64) vin_curb_weight,
    safe_cast(wheelbase_short as int64) wheelbase_short,
    safe_cast(wheelbase_long as int64) wheelbase_long,
    safe_cast(motorcycle_engine_displacement as int64) motorcycle_engine_displacement,
    safe_cast(motorcycle_engine_cycles_code as string) motorcycle_engine_cycles_code,
    safe_cast(motorcycle_dry_weight as int64) motorcycle_dry_weight,
    safe_cast(engine_displacement_cubic_inch as int64) engine_displacement_cubic_inch,
    safe_cast(truck_shipping_weight as int64) truck_shipping_weight,
    safe_cast(
        truck_shipping_weight_variance_code as string
    ) truck_shipping_weight_variance_code,
    safe_cast(truck_weight_rating_code as string) truck_weight_rating_code,
    safe_cast(truck_series as string) truck_series,
    safe_cast(wheels_and_drive_wheels_code as string) wheels_and_drive_wheels_code,
    safe_cast(carburetion as string) carburetion,
    safe_cast(cylinders as string) cylinders,
    safe_cast(fuel_code as string) fuel_code,
    safe_cast(original_tire_size as string) original_tire_size,
    safe_cast(truck_ton_rating as string) truck_ton_rating,
    safe_cast(truck_vin_restraint_type as string) truck_vin_restraint_type,
    safe_cast(vin_character_1 as string) vin_character_1,
    safe_cast(vin_character_2 as string) vin_character_2,
    safe_cast(vin_character_3 as string) vin_character_3,
    safe_cast(vin_character_4 as string) vin_character_4,
    safe_cast(vin_character_5 as string) vin_character_5,
    safe_cast(vin_character_6 as string) vin_character_6,
    safe_cast(vin_character_7 as string) vin_character_7,
    safe_cast(vin_character_8 as string) vin_character_8,
    safe_cast(vin_character_9 as string) vin_character_9,
    safe_cast(vin_character_10 as string) vin_character_10,
    safe_cast(vin_character_11 as string) vin_character_11,
    safe_cast(vin_character_12 as string) vin_character_12
from {{ set_datalake_project("us_dot_fars_staging.vehicle") }} as t
