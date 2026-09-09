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
    safe_cast(driver_maneuver_code as string) driver_maneuver_code
from {{ set_datalake_project("us_dot_fars_staging.vehicle") }} as t
