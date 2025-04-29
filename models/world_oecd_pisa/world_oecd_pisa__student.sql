{{
    config(
        alias="student",
        schema="world_oecd_pisa",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {
                "start": 2015,
                "end": 2022,
                "interval": 1,
            },
        },
        cluster_by=["country_id_iso_3"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(country_id_iso_3 as string) country_id_iso_3,
    safe_cast(country_id_m49 as string) country_id_m49,
    safe_cast(school_id as string) school_id,
    safe_cast(student_id as string) student_id,
    safe_cast(assessment_type as string) assessment_type,
    safe_cast(national_centre_code as string) national_centre_code,
    safe_cast(stratum as string) stratum,
    safe_cast(subregion as string) subregion,
    safe_cast(oecd as string) oecd,
    safe_cast(mode_respondent as string) mode_respondent,
    safe_cast(language_questionnaire as string) language_questionnaire,
    safe_cast(language_assessment as string) language_assessment,
    safe_cast(form_id as string) form_id,
    safe_cast(international_grade as string) international_grade,
    safe_cast(month_birth as int64) month_birth,
    safe_cast(year_birth as int64) year_birth,
    safe_cast(gender as string) gender,
    safe_cast(effort_put_into_test as string) effort_put_into_test,
    safe_cast(effort_would_have_invested as int64) effort_would_have_invested,
    safe_cast(occupation_mother as string) occupation_mother,
    safe_cast(occupation_father as string) occupation_father,
    safe_cast(occupation_self as string) occupation_self,
    safe_cast(grade_compared as int64) grade_compared,
    safe_cast(age as float64) age,
    safe_cast(national_study_programme as string) national_study_programme,
    safe_cast(country_birth_self as string) country_birth_self,
    safe_cast(country_birth_mother as string) country_birth_mother,
    safe_cast(country_birth_father as string) country_birth_father,
    safe_cast(language_home as string) language_home,
    safe_cast(isced_level as string) isced_level,
    safe_cast(isced_designation as string) isced_designation,
    safe_cast(isced_orientation as string) isced_orientation,
    safe_cast(mother_isced as string) mother_isced,
    safe_cast(father_isced as string) father_isced,
    safe_cast(highest_parent_isced as string) highest_parent_isced,
    safe_cast(highest_parent_years_schooling as int64) highest_parent_years_schooling,
    safe_cast(
        mother_isced_alternate_definition as string
    ) mother_isced_alternate_definition,
    safe_cast(
        father_isced_alternate_definition as string
    ) father_isced_alternate_definition,
    safe_cast(
        highest_parent_isced_alternate_definition as string
    ) highest_parent_isced_alternate_definition,
    safe_cast(
        highest_parent_international_years_schooling as int64
    ) highest_parent_international_years_schooling,
    safe_cast(mother_isei as float64) mother_isei,
    safe_cast(father_isei as float64) father_isei,
    safe_cast(
        index_highest_parent_occupation as float64
    ) index_highest_parent_occupation,
    safe_cast(index_immigration_status as string) index_immigration_status,
    safe_cast(
        duration_early_childhood_education_care as string
    ) duration_early_childhood_education_care,
    safe_cast(grade_repetition as string) grade_repetition,
    safe_cast(student_occupational_status as float64) student_occupational_status,
    safe_cast(learning_time_mathematics as int64) learning_time_mathematics,
    safe_cast(learning_time_language as int64) learning_time_language,
    safe_cast(learning_time_science as int64) learning_time_science,
    safe_cast(learning_time_total as int64) learning_time_total,
    safe_cast(school_change as string) school_change,
    safe_cast(number_educational_change as int64) number_educational_change,
    safe_cast(body_mass_index as float64) body_mass_index,
    safe_cast(
        index_economic_social_cultural_status as float64
    ) index_economic_social_cultural_status,
    safe_cast(
        meta_cognition_understanding_remembering as float64
    ) meta_cognition_understanding_remembering,
    safe_cast(meta_cognition_summarising as float64) meta_cognition_summarising,
    safe_cast(
        meta_cognition_assess_credibility as float64
    ) meta_cognition_assess_credibility,
    safe_cast(ict_home as int64) ict_home,
    safe_cast(ict_school as int64) ict_school,
    safe_cast(wle_home_possessions as float64) wle_home_possessions,
    safe_cast(wle_cultural_possessions as float64) wle_cultural_possessions,
    safe_cast(wle_home_educational_resources as float64) wle_home_educational_resources,
    safe_cast(wle_family_wealth as float64) wle_family_wealth,
    safe_cast(wle_ict_resources as float64) wle_ict_resources,
    safe_cast(wle_teacher_support_language as float64) wle_teacher_support_language,
    safe_cast(wle_perceived_feedback as float64) wle_perceived_feedback,
    safe_cast(
        wle_perceived_parents_emotional_support as float64
    ) wle_perceived_parents_emotional_support,
    safe_cast(
        wle_subjective_well_being_sense_belonging as float64
    ) wle_subjective_well_being_sense_belonging,
    safe_cast(
        wle_ict_use_outside_school_leisure as float64
    ) wle_ict_use_outside_school_leisure,
    safe_cast(
        wle_ict_use_outside_school_work as float64
    ) wle_ict_use_outside_school_work,
    safe_cast(wle_ict_use_at_school as float64) wle_ict_use_at_school,
    safe_cast(wle_ict_interest as float64) wle_ict_interest,
    safe_cast(wle_ict_competence as float64) wle_ict_competence,
    safe_cast(wle_ict_autonomy as float64) wle_ict_autonomy,
    safe_cast(wle_ict_social_interaction as float64) wle_ict_social_interaction,
    safe_cast(wle_parental_support_learning as float64) wle_parental_support_learning,
    safe_cast(wle_parents_emotional_support as float64) wle_parents_emotional_support,
    safe_cast(
        wle_parents_perceived_school_quality as float64
    ) wle_parents_perceived_school_quality,
    safe_cast(
        wle_school_policies_parental_involvement as float64
    ) wle_school_policies_parental_involvement,
    safe_cast(
        wle_previous_parental_support_learning as float64
    ) wle_previous_parental_support_learning,
    safe_cast(
        wle_disciplinay_climate_specific_domain as float64
    ) wle_disciplinay_climate_specific_domain,
    safe_cast(
        wle_teacher_directed_instruction as float64
    ) wle_teacher_directed_instruction,
    safe_cast(
        wle_teacher_stimulation_reading as float64
    ) wle_teacher_stimulation_reading,
    safe_cast(wle_adaptation_instruction as float64) wle_adaptation_instruction,
    safe_cast(
        wle_perceived_teachers_interest as float64
    ) wle_perceived_teachers_interest,
    safe_cast(wle_joy_reading as float64) wle_joy_reading,
    safe_cast(
        wle_reading_self_concept_competence as float64
    ) wle_reading_self_concept_competence,
    safe_cast(
        wle_reading_self_concept_difficulty as float64
    ) wle_reading_self_concept_difficulty,
    safe_cast(
        wle_pisa_test_difficulty_perception as float64
    ) wle_pisa_test_difficulty_perception,
    safe_cast(wle_competitiveness_perception as float64) wle_competitiveness_perception,
    safe_cast(wle_cooperation_perception as float64) wle_cooperation_perception,
    safe_cast(
        wle_attitude_towards_school_learning_activities as float64
    ) wle_attitude_towards_school_learning_activities,
    safe_cast(wle_competitiveness as float64) wle_competitiveness,
    safe_cast(wle_work_mastery as float64) wle_work_mastery,
    safe_cast(wle_general_fear_failure as float64) wle_general_fear_failure,
    safe_cast(wle_eudaemonia as float64) wle_eudaemonia,
    safe_cast(
        wle_subjective_well_being_positive_affect as float64
    ) wle_subjective_well_being_positive_affect,
    safe_cast(wle_resilence as float64) wle_resilence,
    safe_cast(wle_mastery_goal_orientation as float64) wle_mastery_goal_orientation,
    safe_cast(
        wle_self_efficacy_regarding_global_issues as float64
    ) wle_self_efficacy_regarding_global_issues,
    safe_cast(
        wle_students_awareness_global_issues as float64
    ) wle_students_awareness_global_issues,
    safe_cast(
        wle_students_attitudes_towards_immigrants as float64
    ) wle_students_attitudes_towards_immigrants,
    safe_cast(
        wle_students_interest_learning_other_cultures as float64
    ) wle_students_interest_learning_other_cultures,
    safe_cast(wle_perspective_taking as float64) wle_perspective_taking,
    safe_cast(wle_cognitive_flexibility as float64) wle_cognitive_flexibility,
    safe_cast(
        wle_respect_people_from_other_cultures as float64
    ) wle_respect_people_from_other_cultures,
    safe_cast(
        wle_intercultural_communication_awareness as float64
    ) wle_intercultural_communication_awareness,
    safe_cast(wle_global_mindedness as float64) wle_global_mindedness,
    safe_cast(
        wle_discriminating_school_climate as float64
    ) wle_discriminating_school_climate,
    safe_cast(
        wle_students_experience_being_bullied as float64
    ) wle_students_experience_being_bullied,
    safe_cast(wle_ict_use_during_lessons as float64) wle_ict_use_during_lessons,
    safe_cast(wle_ict_use_outside_lessons as float64) wle_ict_use_outside_lessons,
    safe_cast(wle_carrer_information as float64) wle_carrer_information,
    safe_cast(
        wle_labour_market_information_by_school as float64
    ) wle_labour_market_information_by_school,
    safe_cast(
        wle_labour_market_information_outside_school as float64
    ) wle_labour_market_information_outside_school,
    safe_cast(
        wle_financial_matters_confidence as float64
    ) wle_financial_matters_confidence,
    safe_cast(
        wle_financial_matters_confidence_digital_devices as float64
    ) wle_financial_matters_confidence_digital_devices,
    safe_cast(
        wle_financial_education_school_lessons as float64
    ) wle_financial_education_school_lessons,
    safe_cast(
        wle_parents_involvement_financial_literacy as float64
    ) wle_parents_involvement_financial_literacy,
    safe_cast(wle_parents_reading_enjoyment as float64) wle_parents_reading_enjoyment,
    safe_cast(
        wle_parents_attitudes_towards_immigrants as float64
    ) wle_parents_attitudes_towards_immigrants,
    safe_cast(
        wle_parents_interest_learking_other_cultures as float64
    ) wle_parents_interest_learking_other_cultures,
    safe_cast(
        wle_parents_awareness_global_issues as float64
    ) wle_parents_awareness_global_issues,
    safe_cast(wle_body_image as float64) wle_body_image,
    safe_cast(wle_parents_social_connections as float64) wle_parents_social_connections,
    safe_cast(
        consider_perspective_before_position as string
    ) consider_perspective_before_position,
    safe_cast(understand_classmates_thinking as string) understand_classmates_thinking,
    safe_cast(view_things_different_angles as string) view_things_different_angles,
    safe_cast(imagine_in_somebody_place as string) imagine_in_somebody_place,
    safe_cast(
        one_correct_position_disagreement as string
    ) one_correct_position_disagreement,
    safe_cast(understand_why_people_behave as string) understand_why_people_behave,
    safe_cast(
        difficult_anticipate_others_think as string
    ) difficult_anticipate_others_think,
    safe_cast(
        envision_friends_points_of_view as string
    ) envision_friends_points_of_view,
    safe_cast(final_student_weight as float64) final_student_weight,
    safe_cast(student_weight_1 as float64) student_weight_1,
    safe_cast(student_weight_2 as float64) student_weight_2,
    safe_cast(student_weight_3 as float64) student_weight_3,
    safe_cast(student_weight_4 as float64) student_weight_4,
    safe_cast(student_weight_5 as float64) student_weight_5,
    safe_cast(student_weight_6 as float64) student_weight_6,
    safe_cast(student_weight_7 as float64) student_weight_7,
    safe_cast(student_weight_8 as float64) student_weight_8,
    safe_cast(student_weight_9 as float64) student_weight_9,
    safe_cast(student_weight_10 as float64) student_weight_10,
    safe_cast(student_weight_11 as float64) student_weight_11,
    safe_cast(student_weight_12 as float64) student_weight_12,
    safe_cast(student_weight_13 as float64) student_weight_13,
    safe_cast(student_weight_14 as float64) student_weight_14,
    safe_cast(student_weight_15 as float64) student_weight_15,
    safe_cast(student_weight_16 as float64) student_weight_16,
    safe_cast(student_weight_17 as float64) student_weight_17,
    safe_cast(student_weight_18 as float64) student_weight_18,
    safe_cast(student_weight_19 as float64) student_weight_19,
    safe_cast(student_weight_20 as float64) student_weight_20,
    safe_cast(student_weight_21 as float64) student_weight_21,
    safe_cast(student_weight_22 as float64) student_weight_22,
    safe_cast(student_weight_23 as float64) student_weight_23,
    safe_cast(student_weight_24 as float64) student_weight_24,
    safe_cast(student_weight_25 as float64) student_weight_25,
    safe_cast(student_weight_26 as float64) student_weight_26,
    safe_cast(student_weight_27 as float64) student_weight_27,
    safe_cast(student_weight_28 as float64) student_weight_28,
    safe_cast(student_weight_29 as float64) student_weight_29,
    safe_cast(student_weight_30 as float64) student_weight_30,
    safe_cast(student_weight_31 as float64) student_weight_31,
    safe_cast(student_weight_32 as float64) student_weight_32,
    safe_cast(student_weight_33 as float64) student_weight_33,
    safe_cast(student_weight_34 as float64) student_weight_34,
    safe_cast(student_weight_35 as float64) student_weight_35,
    safe_cast(student_weight_36 as float64) student_weight_36,
    safe_cast(student_weight_37 as float64) student_weight_37,
    safe_cast(student_weight_38 as float64) student_weight_38,
    safe_cast(student_weight_39 as float64) student_weight_39,
    safe_cast(student_weight_40 as float64) student_weight_40,
    safe_cast(student_weight_41 as float64) student_weight_41,
    safe_cast(student_weight_42 as float64) student_weight_42,
    safe_cast(student_weight_43 as float64) student_weight_43,
    safe_cast(student_weight_44 as float64) student_weight_44,
    safe_cast(student_weight_45 as float64) student_weight_45,
    safe_cast(student_weight_46 as float64) student_weight_46,
    safe_cast(student_weight_47 as float64) student_weight_47,
    safe_cast(student_weight_48 as float64) student_weight_48,
    safe_cast(student_weight_49 as float64) student_weight_49,
    safe_cast(student_weight_50 as float64) student_weight_50,
    safe_cast(student_weight_51 as float64) student_weight_51,
    safe_cast(student_weight_52 as float64) student_weight_52,
    safe_cast(student_weight_53 as float64) student_weight_53,
    safe_cast(student_weight_54 as float64) student_weight_54,
    safe_cast(student_weight_55 as float64) student_weight_55,
    safe_cast(student_weight_56 as float64) student_weight_56,
    safe_cast(student_weight_57 as float64) student_weight_57,
    safe_cast(student_weight_58 as float64) student_weight_58,
    safe_cast(student_weight_59 as float64) student_weight_59,
    safe_cast(student_weight_60 as float64) student_weight_60,
    safe_cast(student_weight_61 as float64) student_weight_61,
    safe_cast(student_weight_62 as float64) student_weight_62,
    safe_cast(student_weight_63 as float64) student_weight_63,
    safe_cast(student_weight_64 as float64) student_weight_64,
    safe_cast(student_weight_65 as float64) student_weight_65,
    safe_cast(student_weight_66 as float64) student_weight_66,
    safe_cast(student_weight_67 as float64) student_weight_67,
    safe_cast(student_weight_68 as float64) student_weight_68,
    safe_cast(student_weight_69 as float64) student_weight_69,
    safe_cast(student_weight_70 as float64) student_weight_70,
    safe_cast(student_weight_71 as float64) student_weight_71,
    safe_cast(student_weight_72 as float64) student_weight_72,
    safe_cast(student_weight_73 as float64) student_weight_73,
    safe_cast(student_weight_74 as float64) student_weight_74,
    safe_cast(student_weight_75 as float64) student_weight_75,
    safe_cast(student_weight_76 as float64) student_weight_76,
    safe_cast(student_weight_77 as float64) student_weight_77,
    safe_cast(student_weight_78 as float64) student_weight_78,
    safe_cast(student_weight_79 as float64) student_weight_79,
    safe_cast(student_weight_80 as float64) student_weight_80,
    safe_cast(randomly_assigned_unit_number as int64) randomly_assigned_unit_number,
    safe_cast(
        randomized_final_variance_stratum as int64
    ) randomized_final_variance_stratum,
    safe_cast(plausible_value_1_mathematics as float64) plausible_value_1_mathematics,
    safe_cast(plausible_value_2_mathematics as float64) plausible_value_2_mathematics,
    safe_cast(plausible_value_3_mathematics as float64) plausible_value_3_mathematics,
    safe_cast(plausible_value_4_mathematics as float64) plausible_value_4_mathematics,
    safe_cast(plausible_value_5_mathematics as float64) plausible_value_5_mathematics,
    safe_cast(plausible_value_6_mathematics as float64) plausible_value_6_mathematics,
    safe_cast(plausible_value_7_mathematics as float64) plausible_value_7_mathematics,
    safe_cast(plausible_value_8_mathematics as float64) plausible_value_8_mathematics,
    safe_cast(plausible_value_9_mathematics as float64) plausible_value_9_mathematics,
    safe_cast(plausible_value_10_mathematics as float64) plausible_value_10_mathematics,
    safe_cast(plausible_value_1_reading as float64) plausible_value_1_reading,
    safe_cast(plausible_value_2_reading as float64) plausible_value_2_reading,
    safe_cast(plausible_value_3_reading as float64) plausible_value_3_reading,
    safe_cast(plausible_value_4_reading as float64) plausible_value_4_reading,
    safe_cast(plausible_value_5_reading as float64) plausible_value_5_reading,
    safe_cast(plausible_value_6_reading as float64) plausible_value_6_reading,
    safe_cast(plausible_value_7_reading as float64) plausible_value_7_reading,
    safe_cast(plausible_value_8_reading as float64) plausible_value_8_reading,
    safe_cast(plausible_value_9_reading as float64) plausible_value_9_reading,
    safe_cast(plausible_value_10_reading as float64) plausible_value_10_reading,
    safe_cast(plausible_value_1_science as float64) plausible_value_1_science,
    safe_cast(plausible_value_2_science as float64) plausible_value_2_science,
    safe_cast(plausible_value_3_science as float64) plausible_value_3_science,
    safe_cast(plausible_value_4_science as float64) plausible_value_4_science,
    safe_cast(plausible_value_5_science as float64) plausible_value_5_science,
    safe_cast(plausible_value_6_science as float64) plausible_value_6_science,
    safe_cast(plausible_value_7_science as float64) plausible_value_7_science,
    safe_cast(plausible_value_8_science as float64) plausible_value_8_science,
    safe_cast(plausible_value_9_science as float64) plausible_value_9_science,
    safe_cast(plausible_value_10_science as float64) plausible_value_10_science,
    safe_cast(senate_weight as float64) senate_weight,
    safe_cast(
        case
            when regexp_contains(date_database_creation, r'^\d{2}[A-Z]{3}\d{2}$')
            then parse_timestamp('%d%b%y', date_database_creation)
            when
                regexp_contains(
                    date_database_creation, r'^\s*\d{2}[A-Z]{3}\d{2}:\d{2}:\d{2}:\d{2}$'
                )
            then parse_timestamp('%d%b%y:%H:%M:%S', trim(date_database_creation))
            else null
        end as date
    ) as date_database_creation
from {{ set_datalake_project("world_oecd_pisa_staging.student") }} as t
