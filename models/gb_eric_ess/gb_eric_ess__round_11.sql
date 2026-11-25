{{
    config(
        schema="gb_eric_ess",
        alias="round_11",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2023, "end": 2028, "interval": 1},
        },
        cluster_by=["country_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(round as int64) round,
    safe_cast(country_id as string) country_id,
    safe_cast(respondent_id as string) respondent_id,
    safe_cast(dataset_title as string) dataset_title,
    safe_cast(edition as string) edition,
    safe_cast(production_date as string) production_date,
    safe_cast(design_weight as float64) design_weight,
    safe_cast(poststratification_weight as float64) poststratification_weight,
    safe_cast(population_size_weight as float64) population_size_weight,
    safe_cast(analysis_weight as float64) analysis_weight,
    safe_cast(minutes_news_politics as int64) minutes_news_politics,
    safe_cast(internet_use_frequency as string) internet_use_frequency,
    safe_cast(minutes_internet_use as int64) minutes_internet_use,
    safe_cast(most_people_can_be_trusted as int64) most_people_can_be_trusted,
    safe_cast(most_people_try_to_be_fair as int64) most_people_try_to_be_fair,
    safe_cast(most_people_helpful as int64) most_people_helpful,
    safe_cast(interest_in_politics as string) interest_in_politics,
    safe_cast(political_system_allows_say as string) political_system_allows_say,
    safe_cast(able_to_take_active_role as string) able_to_take_active_role,
    safe_cast(
        political_system_allows_influence as string
    ) political_system_allows_influence,
    safe_cast(confident_participate_politics as string) confident_participate_politics,
    safe_cast(trust_parliament as int64) trust_parliament,
    safe_cast(trust_legal_system as int64) trust_legal_system,
    safe_cast(trust_police as int64) trust_police,
    safe_cast(trust_politicians as int64) trust_politicians,
    safe_cast(trust_political_parties as int64) trust_political_parties,
    safe_cast(trust_european_parliament as int64) trust_european_parliament,
    safe_cast(trust_united_nations as int64) trust_united_nations,
    safe_cast(voted_last_national_election as string) voted_last_national_election,
    safe_cast(party_voted_austria as string) party_voted_austria,
    safe_cast(party_voted_belgium as string) party_voted_belgium,
    safe_cast(party_voted_bulgaria as string) party_voted_bulgaria,
    safe_cast(party_voted_croatia as string) party_voted_croatia,
    safe_cast(party_voted_cyprus as string) party_voted_cyprus,
    safe_cast(party_voted_estonia_10 as string) party_voted_estonia_10,
    safe_cast(party_voted_finland as string) party_voted_finland,
    safe_cast(party_voted_france_ballot_1 as string) party_voted_france_ballot_1,
    safe_cast(party_voted_1_germany as string) party_voted_1_germany,
    safe_cast(party_voted_2_germany as string) party_voted_2_germany,
    safe_cast(party_voted_greece as string) party_voted_greece,
    safe_cast(party_voted_hungary as string) party_voted_hungary,
    safe_cast(party_voted_iceland as string) party_voted_iceland,
    safe_cast(
        party_voted_ireland_derived_from_1st as string
    ) party_voted_ireland_derived_from_1st,
    safe_cast(party_voted_israel as string) party_voted_israel,
    safe_cast(party_voted_italy as string) party_voted_italy,
    safe_cast(party_voted_latvia as string) party_voted_latvia,
    safe_cast(
        party_voted_1_lithuania_first_vote_party as string
    ) party_voted_1_lithuania_first_vote_party,
    safe_cast(
        party_voted_2_lithuania_second_vote_party as string
    ) party_voted_2_lithuania_second_vote_party,
    safe_cast(
        party_voted_3_lithuania_third_vote_party as string
    ) party_voted_3_lithuania_third_vote_party,
    safe_cast(party_voted_montenegro as string) party_voted_montenegro,
    safe_cast(party_voted_netherlands as string) party_voted_netherlands,
    safe_cast(party_voted_norway as string) party_voted_norway,
    safe_cast(party_voted_poland as string) party_voted_poland,
    safe_cast(party_voted_portugal as string) party_voted_portugal,
    safe_cast(party_voted_serbia as string) party_voted_serbia,
    safe_cast(party_voted_slovakia as string) party_voted_slovakia,
    safe_cast(party_voted_slovenia as string) party_voted_slovenia,
    safe_cast(party_voted_spain as string) party_voted_spain,
    safe_cast(party_voted_sweden_6 as string) party_voted_sweden_6,
    safe_cast(party_voted_switzerland as string) party_voted_switzerland,
    safe_cast(party_voted_ukraine_ballot_2_2 as string) party_voted_ukraine_ballot_2_2,
    safe_cast(party_voted_united_kingdom as string) party_voted_united_kingdom,
    safe_cast(contacted_politician as string) contacted_politician,
    safe_cast(donated_to_party as string) donated_to_party,
    safe_cast(worn_campaign_badge as string) worn_campaign_badge,
    safe_cast(signed_petition as string) signed_petition,
    safe_cast(
        participated_lawful_demonstration as string
    ) participated_lawful_demonstration,
    safe_cast(boycotted_products as string) boycotted_products,
    safe_cast(posted_shared_politics_online as string) posted_shared_politics_online,
    safe_cast(
        volunteered_not_profit_charitable_organisation as string
    ) volunteered_not_profit_charitable_organisation,
    safe_cast(feel_close_to_party as string) feel_close_to_party,
    safe_cast(party_feel_close_austria as string) party_feel_close_austria,
    safe_cast(party_feel_close_belgium as string) party_feel_close_belgium,
    safe_cast(party_feel_close_bulgaria as string) party_feel_close_bulgaria,
    safe_cast(party_feel_close_croatia as string) party_feel_close_croatia,
    safe_cast(party_feel_close_cyprus as string) party_feel_close_cyprus,
    safe_cast(party_feel_close_estonia_10 as string) party_feel_close_estonia_10,
    safe_cast(party_feel_close_finland as string) party_feel_close_finland,
    safe_cast(party_feel_close_france as string) party_feel_close_france,
    safe_cast(party_feel_close_germany as string) party_feel_close_germany,
    safe_cast(party_feel_close_greece as string) party_feel_close_greece,
    safe_cast(party_feel_close_hungary as string) party_feel_close_hungary,
    safe_cast(party_feel_close_iceland as string) party_feel_close_iceland,
    safe_cast(party_feel_close_ireland as string) party_feel_close_ireland,
    safe_cast(party_feel_close_israel as string) party_feel_close_israel,
    safe_cast(party_feel_close_italy as string) party_feel_close_italy,
    safe_cast(party_feel_close_latvia as string) party_feel_close_latvia,
    safe_cast(party_feel_close_lithuania as string) party_feel_close_lithuania,
    safe_cast(party_feel_close_montenegro as string) party_feel_close_montenegro,
    safe_cast(party_feel_close_netherlands as string) party_feel_close_netherlands,
    safe_cast(party_feel_close_norway as string) party_feel_close_norway,
    safe_cast(party_feel_close_poland as string) party_feel_close_poland,
    safe_cast(party_feel_close_portugal as string) party_feel_close_portugal,
    safe_cast(party_feel_close_serbia as string) party_feel_close_serbia,
    safe_cast(party_feel_close_slovakia as string) party_feel_close_slovakia,
    safe_cast(party_feel_close_slovenia as string) party_feel_close_slovenia,
    safe_cast(party_feel_close_spain as string) party_feel_close_spain,
    safe_cast(party_feel_close_sweden_6 as string) party_feel_close_sweden_6,
    safe_cast(party_feel_close_switzerland as string) party_feel_close_switzerland,
    safe_cast(party_feel_close_ukraine_6 as string) party_feel_close_ukraine_6,
    safe_cast(
        party_feel_close_united_kingdom as string
    ) party_feel_close_united_kingdom,
    safe_cast(how_close_to_party as string) how_close_to_party,
    safe_cast(left_right_scale as int64) left_right_scale,
    safe_cast(life_satisfaction as int64) life_satisfaction,
    safe_cast(satisfaction_economy as int64) satisfaction_economy,
    safe_cast(satisfaction_government as int64) satisfaction_government,
    safe_cast(satisfaction_democracy as int64) satisfaction_democracy,
    safe_cast(satisfaction_education as int64) satisfaction_education,
    safe_cast(satisfaction_health_services as int64) satisfaction_health_services,
    safe_cast(
        government_reduce_income_differences as string
    ) government_reduce_income_differences,
    safe_cast(gays_free_to_live_as_wish as string) gays_free_to_live_as_wish,
    safe_cast(ashamed_gay_family_member as string) ashamed_gay_family_member,
    safe_cast(gay_couples_adopt_rights as string) gay_couples_adopt_rights,
    safe_cast(european_unification_go_further as int64) european_unification_go_further,
    safe_cast(
        obedience_respect_authority_most_important_virtues as string
    ) obedience_respect_authority_most_important_virtues,
    safe_cast(
        country_needs_most_loyalty_towards_its_leaders as string
    ) country_needs_most_loyalty_towards_its_leaders,
    safe_cast(
        allow_immigrants_same_ethnicity as string
    ) allow_immigrants_same_ethnicity,
    safe_cast(
        allow_immigrants_different_ethnicity as string
    ) allow_immigrants_different_ethnicity,
    safe_cast(
        allow_immigrants_poorer_countries as string
    ) allow_immigrants_poorer_countries,
    safe_cast(immigration_good_for_economy as int64) immigration_good_for_economy,
    safe_cast(immigration_undermines_culture as int64) immigration_undermines_culture,
    safe_cast(immigrants_make_country_better as int64) immigrants_make_country_better,
    safe_cast(happiness as int64) happiness,
    safe_cast(frequency_social_meetings as string) frequency_social_meetings,
    safe_cast(number_people_discuss_personal as string) number_people_discuss_personal,
    safe_cast(social_activities_vs_peers as string) social_activities_vs_peers,
    safe_cast(victim_of_crime as string) victim_of_crime,
    safe_cast(feeling_safe_walking_dark as string) feeling_safe_walking_dark,
    safe_cast(subjective_general_health as string) subjective_general_health,
    safe_cast(hampered_by_illness as string) hampered_by_illness,
    safe_cast(emotional_attachment_country as int64) emotional_attachment_country,
    safe_cast(emotional_attachment_europe as int64) emotional_attachment_europe,
    safe_cast(belongs_to_religion as string) belongs_to_religion,
    safe_cast(
        religion_denomination_belonging_present as string
    ) religion_denomination_belonging_present,
    safe_cast(
        religion_denomination_belonging_present_austria as string
    ) religion_denomination_belonging_present_austria,
    safe_cast(
        religion_denomination_belonging_present_cyprus_2 as string
    ) religion_denomination_belonging_present_cyprus_2,
    safe_cast(
        religion_denomination_belonging_present_finland as string
    ) religion_denomination_belonging_present_finland,
    safe_cast(
        religion_denomination_belonging_present_germany as string
    ) religion_denomination_belonging_present_germany,
    safe_cast(
        religion_denomination_belonging_present_greece as string
    ) religion_denomination_belonging_present_greece,
    safe_cast(
        religion_denomination_belonging_present_hungary as string
    ) religion_denomination_belonging_present_hungary,
    safe_cast(
        religion_denomination_belonging_present_iceland as string
    ) religion_denomination_belonging_present_iceland,
    safe_cast(
        religion_denomination_belonging_present_ireland as string
    ) religion_denomination_belonging_present_ireland,
    safe_cast(
        religion_denomination_belonging_present_latvia as string
    ) religion_denomination_belonging_present_latvia,
    safe_cast(
        religion_denomination_belonging_present_lithuania as string
    ) religion_denomination_belonging_present_lithuania,
    safe_cast(
        religion_denomination_belonging_present_montenegro as string
    ) religion_denomination_belonging_present_montenegro,
    safe_cast(
        religion_denomination_belonging_present_netherlands as string
    ) religion_denomination_belonging_present_netherlands,
    safe_cast(
        religion_denomination_belonging_present_norway as string
    ) religion_denomination_belonging_present_norway,
    safe_cast(
        religion_denomination_belonging_present_poland as string
    ) religion_denomination_belonging_present_poland,
    safe_cast(
        religion_denomination_belonging_present_portugal as string
    ) religion_denomination_belonging_present_portugal,
    safe_cast(
        religion_denomination_belonging_present_serbia as string
    ) religion_denomination_belonging_present_serbia,
    safe_cast(
        religion_denomination_belonging_present_slovakia as string
    ) religion_denomination_belonging_present_slovakia,
    safe_cast(
        religion_denomination_belonging_present_sweden as string
    ) religion_denomination_belonging_present_sweden,
    safe_cast(
        religion_denomination_belonging_present_switzerland as string
    ) religion_denomination_belonging_present_switzerland,
    safe_cast(
        religion_denomination_belonging_present_ukraine_2 as string
    ) religion_denomination_belonging_present_ukraine_2,
    safe_cast(
        religion_denomination_belonging_present_united_kingdom as string
    ) religion_denomination_belonging_present_united_kingdom,
    safe_cast(
        ever_belonging_particular_religion_denomination as string
    ) ever_belonging_particular_religion_denomination,
    safe_cast(
        religion_denomination_belonging_past as string
    ) religion_denomination_belonging_past,
    safe_cast(
        religion_denomination_belonging_past_austria as string
    ) religion_denomination_belonging_past_austria,
    safe_cast(
        religion_denomination_belonging_past_cyprus_2 as string
    ) religion_denomination_belonging_past_cyprus_2,
    safe_cast(
        religion_denomination_belonging_past_finland as string
    ) religion_denomination_belonging_past_finland,
    safe_cast(
        religion_denomination_belonging_past_germany as string
    ) religion_denomination_belonging_past_germany,
    safe_cast(
        religion_denomination_belonging_past_greece as string
    ) religion_denomination_belonging_past_greece,
    safe_cast(
        religion_denomination_belonging_past_hungary as string
    ) religion_denomination_belonging_past_hungary,
    safe_cast(
        religion_denomination_belonging_past_iceland as string
    ) religion_denomination_belonging_past_iceland,
    safe_cast(
        religion_denomination_belonging_past_ireland as string
    ) religion_denomination_belonging_past_ireland,
    safe_cast(
        religion_denomination_belonging_past_latvia as string
    ) religion_denomination_belonging_past_latvia,
    safe_cast(
        religion_denomination_belonging_past_lithuania as string
    ) religion_denomination_belonging_past_lithuania,
    safe_cast(
        religion_denomination_belonging_past_montenegro as string
    ) religion_denomination_belonging_past_montenegro,
    safe_cast(
        religion_denomination_belonging_past_netherlands as string
    ) religion_denomination_belonging_past_netherlands,
    safe_cast(
        religion_denomination_belonging_past_norway as string
    ) religion_denomination_belonging_past_norway,
    safe_cast(
        religion_denomination_belonging_past_poland as string
    ) religion_denomination_belonging_past_poland,
    safe_cast(
        religion_denomination_belonging_past_portugal as string
    ) religion_denomination_belonging_past_portugal,
    safe_cast(
        religion_denomination_belonging_past_serbia as string
    ) religion_denomination_belonging_past_serbia,
    safe_cast(
        religion_denomination_belonging_past_slovakia as string
    ) religion_denomination_belonging_past_slovakia,
    safe_cast(
        religion_denomination_belonging_past_sweden as string
    ) religion_denomination_belonging_past_sweden,
    safe_cast(
        religion_denomination_belonging_past_switzerland as string
    ) religion_denomination_belonging_past_switzerland,
    safe_cast(
        religion_denomination_belonging_past_ukraine_2 as string
    ) religion_denomination_belonging_past_ukraine_2,
    safe_cast(
        religion_denomination_belonging_past_united_kingdom as string
    ) religion_denomination_belonging_past_united_kingdom,
    safe_cast(how_religious as int64) how_religious,
    safe_cast(religious_attendance_frequency as string) religious_attendance_frequency,
    safe_cast(frequency_of_praying as string) frequency_of_praying,
    safe_cast(member_discriminated_group as string) member_discriminated_group,
    safe_cast(
        discrimination_group_colour_race as string
    ) discrimination_group_colour_race,
    safe_cast(
        discrimination_group_nationality as string
    ) discrimination_group_nationality,
    safe_cast(discrimination_group_religion as string) discrimination_group_religion,
    safe_cast(discrimination_group_language as string) discrimination_group_language,
    safe_cast(
        discrimination_group_ethnic_group as string
    ) discrimination_group_ethnic_group,
    safe_cast(discrimination_group_age as string) discrimination_group_age,
    safe_cast(discrimination_group_gender as string) discrimination_group_gender,
    safe_cast(discrimination_group_sexuality as string) discrimination_group_sexuality,
    safe_cast(
        discrimination_group_disability as string
    ) discrimination_group_disability,
    safe_cast(
        discrimination_group_other_grounds as string
    ) discrimination_group_other_grounds,
    safe_cast(discrimination_group_dont_know as string) discrimination_group_dont_know,
    safe_cast(discrimination_group_refusal as string) discrimination_group_refusal,
    safe_cast(
        discrimination_group_not_applicable as string
    ) discrimination_group_not_applicable,
    safe_cast(discrimination_group_no_answer as string) discrimination_group_no_answer,
    safe_cast(citizen_of_country as string) citizen_of_country,
    safe_cast(born_in_country as string) born_in_country,
    safe_cast(country_birth as string) country_birth,
    safe_cast(year_arrived_country as int64) year_arrived_country,
    safe_cast(language_home_first as string) language_home_first,
    safe_cast(language_home_second as string) language_home_second,
    safe_cast(
        feel_part_same_race_ethnic_group as string
    ) feel_part_same_race_ethnic_group,
    safe_cast(father_born_in_country as string) father_born_in_country,
    safe_cast(country_birth_father as string) country_birth_father,
    safe_cast(mother_born_in_country as string) mother_born_in_country,
    safe_cast(country_birth_mother as string) country_birth_mother,
    safe_cast(
        climate_change_caused_natural_processes_human as string
    ) climate_change_caused_natural_processes_human,
    safe_cast(
        what_extent_feel_personal_responsibility_reduce as int64
    ) what_extent_feel_personal_responsibility_reduce,
    safe_cast(worried_climate_change as string) worried_climate_change,
    safe_cast(
        administration_reduce_climate_change as string
    ) administration_reduce_climate_change,
    safe_cast(
        imagine_large_numbers_people_limit_energy as int64
    ) imagine_large_numbers_people_limit_energy,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use as int64
    ) likelihood_large_numbers_people_limit_energy_use,
    safe_cast(
        likelihood_governments_enough_countries_take_action as int64
    ) likelihood_governments_enough_countries_take_action,
    safe_cast(
        imagine_large_numbers_people_limit_energy_2 as string
    ) imagine_large_numbers_people_limit_energy_2,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_2 as string
    ) likelihood_large_numbers_people_limit_energy_use_2,
    safe_cast(
        likelihood_governments_enough_countries_take_action_2 as string
    ) likelihood_governments_enough_countries_take_action_2,
    safe_cast(
        imagine_large_numbers_people_limit_energy_3 as string
    ) imagine_large_numbers_people_limit_energy_3,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_3 as string
    ) likelihood_large_numbers_people_limit_energy_use_3,
    safe_cast(
        likelihood_governments_enough_countries_take_action_3 as string
    ) likelihood_governments_enough_countries_take_action_3,
    safe_cast(would_vote as string) would_vote,
    safe_cast(would_vote_2 as string) would_vote_2,
    safe_cast(
        much_control_over_life_general_nowadays as int64
    ) much_control_over_life_general_nowadays,
    safe_cast(
        frequency_eat_fruit_excluding_drinking_juice as string
    ) frequency_eat_fruit_excluding_drinking_juice,
    safe_cast(
        frequency_eat_vegetables_salad_excluding_potatoes as string
    ) frequency_eat_vegetables_salad_excluding_potatoes,
    safe_cast(
        do_sports_other_physical_activity_number_7_days as int64
    ) do_sports_other_physical_activity_number_7_days,
    safe_cast(cigarette_smoking_behaviour as string) cigarette_smoking_behaviour,
    safe_cast(frequency_drink_alcohol as string) frequency_drink_alcohol,
    safe_cast(
        grams_alcohol_time_drinking_weekday_monday as int64
    ) grams_alcohol_time_drinking_weekday_monday,
    safe_cast(
        grams_alcohol_time_drinking_weekend_day as int64
    ) grams_alcohol_time_drinking_weekend_day,
    safe_cast(
        interviewer_code_gender_respondent as string
    ) interviewer_code_gender_respondent,
    safe_cast(
        frequency_binge_drinking_men_women_12_months as string
    ) frequency_binge_drinking_men_women_12_months,
    safe_cast(height_respondent_cm as int64) height_respondent_cm,
    safe_cast(weight_respondent_kg as int64) weight_respondent_kg,
    safe_cast(
        discussed_health_with_general_practitioner as string
    ) discussed_health_with_general_practitioner,
    safe_cast(
        discussed_health_with_medical_specialist as string
    ) discussed_health_with_medical_specialist,
    safe_cast(
        discussed_health_with_none_these as string
    ) discussed_health_with_none_these,
    safe_cast(discussed_health_with_refusal as string) discussed_health_with_refusal,
    safe_cast(
        discussed_health_with_dont_know as string
    ) discussed_health_with_dont_know,
    safe_cast(
        discussed_health_with_no_answer as string
    ) discussed_health_with_no_answer,
    safe_cast(
        unable_get_medical_consultation_treatment_12 as string
    ) unable_get_medical_consultation_treatment_12,
    safe_cast(
        no_medical_reason_could_not_pay as string
    ) no_medical_reason_could_not_pay,
    safe_cast(
        no_medical_reason_could_not_take_time_off_work as string
    ) no_medical_reason_could_not_take_time_off_work,
    safe_cast(
        no_medical_reason_other_commitments as string
    ) no_medical_reason_other_commitments,
    safe_cast(
        no_medical_reason_not_available_where_live as string
    ) no_medical_reason_not_available_where_live,
    safe_cast(
        no_medical_reason_waiting_list_too_long as string
    ) no_medical_reason_waiting_list_too_long,
    safe_cast(
        no_medical_reason_no_appointments_available as string
    ) no_medical_reason_no_appointments_available,
    safe_cast(no_medical_reason_other as string) no_medical_reason_other,
    safe_cast(
        no_medical_reason_not_applicable as string
    ) no_medical_reason_not_applicable,
    safe_cast(no_medical_reason_refusal as string) no_medical_reason_refusal,
    safe_cast(no_medical_reason_dont_know as string) no_medical_reason_dont_know,
    safe_cast(no_medical_reason_no_answer as string) no_medical_reason_no_answer,
    safe_cast(
        never_unable_get_medical_consultation_treatment as string
    ) never_unable_get_medical_consultation_treatment,
    safe_cast(
        looking_after_helping_family_members_friends as string
    ) looking_after_helping_family_members_friends,
    safe_cast(
        hours_week_looking_after_helping_family as string
    ) hours_week_looking_after_helping_family,
    safe_cast(treatment_acupuncture as string) treatment_acupuncture,
    safe_cast(treatment_acupressure as string) treatment_acupressure,
    safe_cast(treatment_chinese_medicine as string) treatment_chinese_medicine,
    safe_cast(treatment_chiropractics as string) treatment_chiropractics,
    safe_cast(treatment_osteopathy as string) treatment_osteopathy,
    safe_cast(treatment_homeopathy as string) treatment_homeopathy,
    safe_cast(treatment_herbal_treatment as string) treatment_herbal_treatment,
    safe_cast(treatment_hypnotherapy as string) treatment_hypnotherapy,
    safe_cast(treatment_massage_therapy as string) treatment_massage_therapy,
    safe_cast(treatment_physiotherapy as string) treatment_physiotherapy,
    safe_cast(treatment_reflexology as string) treatment_reflexology,
    safe_cast(treatment_spiritual_healing as string) treatment_spiritual_healing,
    safe_cast(treatment_none_these as string) treatment_none_these,
    safe_cast(treatment_refusal as string) treatment_refusal,
    safe_cast(treatment_dont_know as string) treatment_dont_know,
    safe_cast(treatment_no_answer as string) treatment_no_answer,
    safe_cast(
        felt_depressed_frequency_past_week as string
    ) felt_depressed_frequency_past_week,
    safe_cast(
        felt_everything_did_effort_frequency_past_week as string
    ) felt_everything_did_effort_frequency_past_week,
    safe_cast(
        sleep_restless_frequency_past_week as string
    ) sleep_restless_frequency_past_week,
    safe_cast(were_happy_frequency_past_week as string) were_happy_frequency_past_week,
    safe_cast(
        felt_lonely_frequency_past_week as string
    ) felt_lonely_frequency_past_week,
    safe_cast(
        enjoyed_life_frequency_past_week as string
    ) enjoyed_life_frequency_past_week,
    safe_cast(felt_sad_frequency_past_week as string) felt_sad_frequency_past_week,
    safe_cast(
        could_not_get_going_frequency_past_week as string
    ) could_not_get_going_frequency_past_week,
    safe_cast(
        health_problem_heart_circulation_problem as string
    ) health_problem_heart_circulation_problem,
    safe_cast(
        health_problem_high_blood_pressure as string
    ) health_problem_high_blood_pressure,
    safe_cast(
        health_problem_breathing_problems as string
    ) health_problem_breathing_problems,
    safe_cast(health_problem_allergies as string) health_problem_allergies,
    safe_cast(health_problem_back_neck_pain as string) health_problem_back_neck_pain,
    safe_cast(
        health_problem_muscular_joint_pain_hand_arm as string
    ) health_problem_muscular_joint_pain_hand_arm,
    safe_cast(
        health_problem_muscular_joint_pain_foot_leg as string
    ) health_problem_muscular_joint_pain_foot_leg,
    safe_cast(
        health_problem_stomach_digestion_related as string
    ) health_problem_stomach_digestion_related,
    safe_cast(
        health_problem_skin_condition_related as string
    ) health_problem_skin_condition_related,
    safe_cast(
        health_problem_severe_headaches as string
    ) health_problem_severe_headaches,
    safe_cast(health_problem_diabetes as string) health_problem_diabetes,
    safe_cast(health_problem_none_these as string) health_problem_none_these,
    safe_cast(health_problem_refusal as string) health_problem_refusal,
    safe_cast(health_problem_dont_know as string) health_problem_dont_know,
    safe_cast(health_problem_no_answer as string) health_problem_no_answer,
    safe_cast(
        health_problem_hampered_heart_circulation_problem as string
    ) health_problem_hampered_heart_circulation_problem,
    safe_cast(
        health_problem_hampered_high_blood_pressure as string
    ) health_problem_hampered_high_blood_pressure,
    safe_cast(
        health_problem_hampered_breathing_problems as string
    ) health_problem_hampered_breathing_problems,
    safe_cast(
        health_problem_hampered_allergies as string
    ) health_problem_hampered_allergies,
    safe_cast(
        health_problem_hampered_back_neck_pain as string
    ) health_problem_hampered_back_neck_pain,
    safe_cast(
        health_problem_hampered_muscular_joint_pain as string
    ) health_problem_hampered_muscular_joint_pain,
    safe_cast(
        health_problem_hampered_muscular_joint_pain_2 as string
    ) health_problem_hampered_muscular_joint_pain_2,
    safe_cast(
        health_problem_hampered_stomach_digestion_related as string
    ) health_problem_hampered_stomach_digestion_related,
    safe_cast(
        health_problem_hampered_skin_condition_related as string
    ) health_problem_hampered_skin_condition_related,
    safe_cast(
        health_problem_hampered_severe_headaches as string
    ) health_problem_hampered_severe_headaches,
    safe_cast(
        health_problem_hampered_diabetes as string
    ) health_problem_hampered_diabetes,
    safe_cast(
        health_problem_hampered_none_these as string
    ) health_problem_hampered_none_these,
    safe_cast(
        health_problem_hampered_not_applicable as string
    ) health_problem_hampered_not_applicable,
    safe_cast(
        health_problem_hampered_refusal as string
    ) health_problem_hampered_refusal,
    safe_cast(
        health_problem_hampered_dont_know as string
    ) health_problem_hampered_dont_know,
    safe_cast(
        health_problem_hampered_no_answer as string
    ) health_problem_hampered_no_answer,
    safe_cast(
        have_had_any_health_problems_listed as string
    ) have_had_any_health_problems_listed,
    safe_cast(
        long_respondent_have_cancer_free as string
    ) long_respondent_have_cancer_free,
    safe_cast(
        serious_conflict_between_people_household_when as string
    ) serious_conflict_between_people_household_when,
    safe_cast(
        severe_financial_difficulties_family_when_growing as string
    ) severe_financial_difficulties_family_when_growing,
    safe_cast(
        job_exposed_to_vibrations_from_hand as string
    ) job_exposed_to_vibrations_from_hand,
    safe_cast(
        job_exposed_to_tiring_painful_positions as string
    ) job_exposed_to_tiring_painful_positions,
    safe_cast(
        job_exposed_to_manually_lifting_moving_people as string
    ) job_exposed_to_manually_lifting_moving_people,
    safe_cast(
        job_exposed_to_manually_carrying_moving as string
    ) job_exposed_to_manually_carrying_moving,
    safe_cast(job_exposed_to_none_these as string) job_exposed_to_none_these,
    safe_cast(job_exposed_to_not_applicable as string) job_exposed_to_not_applicable,
    safe_cast(job_exposed_to_refusal as string) job_exposed_to_refusal,
    safe_cast(job_exposed_to_dont_know as string) job_exposed_to_dont_know,
    safe_cast(job_exposed_to_no_answer as string) job_exposed_to_no_answer,
    safe_cast(job_exposed_to_very_loud_noise as string) job_exposed_to_very_loud_noise,
    safe_cast(
        job_exposed_to_very_hot_temperatures as string
    ) job_exposed_to_very_hot_temperatures,
    safe_cast(
        job_exposed_to_very_cold_temperatures as string
    ) job_exposed_to_very_cold_temperatures,
    safe_cast(
        job_exposed_to_radiation_such_x_rays as string
    ) job_exposed_to_radiation_such_x_rays,
    safe_cast(
        job_exposed_to_contact_chemical_products as string
    ) job_exposed_to_contact_chemical_products,
    safe_cast(
        job_exposed_to_breathing_other_types as string
    ) job_exposed_to_breathing_other_types,
    safe_cast(job_exposed_to_none_these_2 as string) job_exposed_to_none_these_2,
    safe_cast(
        job_exposed_to_not_applicable_2 as string
    ) job_exposed_to_not_applicable_2,
    safe_cast(job_exposed_to_refusal_2 as string) job_exposed_to_refusal_2,
    safe_cast(job_exposed_to_dont_know_2 as string) job_exposed_to_dont_know_2,
    safe_cast(job_exposed_to_no_answer_2 as string) job_exposed_to_no_answer_2,
    safe_cast(
        non_binary_gender_option_respondent_says_best as string
    ) non_binary_gender_option_respondent_says_best,
    safe_cast(i_like_take_risks_what_extent as string) i_like_take_risks_what_extent,
    safe_cast(i_like_leader_what_extent as string) i_like_leader_what_extent,
    safe_cast(i_am_sensitive_others_needs as string) i_am_sensitive_others_needs,
    safe_cast(
        i_act_compassionately_towards_others_what_extent as string
    ) i_act_compassionately_towards_others_what_extent,
    safe_cast(masculine_respondent_feels as string) masculine_respondent_feels,
    safe_cast(feminine_respondent_feels as string) feminine_respondent_feels,
    safe_cast(
        important_man_woman_way_respondent_think as string
    ) important_man_woman_way_respondent_think,
    safe_cast(
        unfairly_treated_when_visiting_doctor_seeking as string
    ) unfairly_treated_when_visiting_doctor_seeking,
    safe_cast(
        unfairly_treated_hiring_pay_promotion_work as string
    ) unfairly_treated_hiring_pay_promotion_work,
    safe_cast(
        unfairly_treated_police_because_man_woman as string
    ) unfairly_treated_police_because_man_woman,
    safe_cast(
        women_men_treated_equally_fairly_when as string
    ) women_men_treated_equally_fairly_when,
    safe_cast(
        women_men_treated_equally_fairly_hiring as string
    ) women_men_treated_equally_fairly_hiring,
    safe_cast(fair_police as string) fair_police,
    safe_cast(bad_good_family_life as string) bad_good_family_life,
    safe_cast(bad_good_politics as string) bad_good_politics,
    safe_cast(bad_good_businesses as string) bad_good_businesses,
    safe_cast(bad_good_economy as string) bad_good_economy,
    safe_cast(
        dividing_number_seats_parliament_equally_between as string
    ) dividing_number_seats_parliament_equally_between,
    safe_cast(
        require_both_parents_take_equal_periods as string
    ) require_both_parents_take_equal_periods,
    safe_cast(
        firing_employees_who_make_insulting_comments as string
    ) firing_employees_who_make_insulting_comments,
    safe_cast(
        making_businesses_pay_fine_when_they as string
    ) making_businesses_pay_fine_when_they,
    safe_cast(
        frequency_women_seek_gain_power_getting as string
    ) frequency_women_seek_gain_power_getting,
    safe_cast(
        frequency_women_get_easily_offended as string
    ) frequency_women_get_easily_offended,
    safe_cast(
        frequency_women_paid_less_than_men_same_work as string
    ) frequency_women_paid_less_than_men_same_work,
    safe_cast(
        frequency_women_exaggerate_claims_sexual_harassment as string
    ) frequency_women_exaggerate_claims_sexual_harassment,
    safe_cast(women_should_protected_men as string) women_should_protected_men,
    safe_cast(
        women_tend_have_better_sense_what as string
    ) women_tend_have_better_sense_what,
    safe_cast(
        number_people_living_regularly_member_household as int64
    ) number_people_living_regularly_member_household,
    safe_cast(gender as string) gender,
    safe_cast(household_member_2_gender as string) household_member_2_gender,
    safe_cast(household_member_3_gender as string) household_member_3_gender,
    safe_cast(household_member_4_gender as string) household_member_4_gender,
    safe_cast(household_member_5_gender as string) household_member_5_gender,
    safe_cast(household_member_6_gender as string) household_member_6_gender,
    safe_cast(household_member_7_gender as string) household_member_7_gender,
    safe_cast(household_member_8_gender as string) household_member_8_gender,
    safe_cast(household_member_9_gender as string) household_member_9_gender,
    safe_cast(household_member_10_gender as string) household_member_10_gender,
    safe_cast(household_member_11_gender as string) household_member_11_gender,
    safe_cast(household_member_12_gender as string) household_member_12_gender,
    safe_cast(household_member_13_gender as string) household_member_13_gender,
    safe_cast(year_of_birth as int64) year_of_birth,
    safe_cast(age as int64) age,
    safe_cast(age_group_post_coded as string) age_group_post_coded,
    safe_cast(
        household_member_2_year_of_birth as int64
    ) household_member_2_year_of_birth,
    safe_cast(
        household_member_3_year_of_birth as int64
    ) household_member_3_year_of_birth,
    safe_cast(
        household_member_4_year_of_birth as int64
    ) household_member_4_year_of_birth,
    safe_cast(
        household_member_5_year_of_birth as int64
    ) household_member_5_year_of_birth,
    safe_cast(
        household_member_6_year_of_birth as int64
    ) household_member_6_year_of_birth,
    safe_cast(
        household_member_7_year_of_birth as int64
    ) household_member_7_year_of_birth,
    safe_cast(
        household_member_8_year_of_birth as int64
    ) household_member_8_year_of_birth,
    safe_cast(
        household_member_9_year_of_birth as int64
    ) household_member_9_year_of_birth,
    safe_cast(
        household_member_10_year_of_birth as int64
    ) household_member_10_year_of_birth,
    safe_cast(
        household_member_11_year_of_birth as int64
    ) household_member_11_year_of_birth,
    safe_cast(
        household_member_12_year_of_birth as int64
    ) household_member_12_year_of_birth,
    safe_cast(
        household_member_13_year_of_birth as int64
    ) household_member_13_year_of_birth,
    safe_cast(
        household_member_2_relationship as string
    ) household_member_2_relationship,
    safe_cast(
        household_member_3_relationship as string
    ) household_member_3_relationship,
    safe_cast(
        household_member_4_relationship as string
    ) household_member_4_relationship,
    safe_cast(
        household_member_5_relationship as string
    ) household_member_5_relationship,
    safe_cast(
        household_member_6_relationship as string
    ) household_member_6_relationship,
    safe_cast(
        household_member_7_relationship as string
    ) household_member_7_relationship,
    safe_cast(
        household_member_8_relationship as string
    ) household_member_8_relationship,
    safe_cast(
        household_member_9_relationship as string
    ) household_member_9_relationship,
    safe_cast(
        household_member_10_relationship as string
    ) household_member_10_relationship,
    safe_cast(
        household_member_11_relationship as string
    ) household_member_11_relationship,
    safe_cast(
        household_member_12_relationship as string
    ) household_member_12_relationship,
    safe_cast(
        household_member_13_relationship as string
    ) household_member_13_relationship,
    safe_cast(
        household_member_15_relationship as string
    ) household_member_15_relationship,
    safe_cast(
        relationship_husband_wife_partner_currently_living as string
    ) relationship_husband_wife_partner_currently_living,
    safe_cast(
        relationship_husband_wife_partner_currently_living_2 as string
    ) relationship_husband_wife_partner_currently_living_2,
    safe_cast(
        ever_lived_partner_without_married as string
    ) ever_lived_partner_without_married,
    safe_cast(
        ever_divorced_had_civil_union_dissolved as string
    ) ever_divorced_had_civil_union_dissolved,
    safe_cast(legal_marital_status as string) legal_marital_status,
    safe_cast(
        legal_marital_status_united_kingdom as string
    ) legal_marital_status_united_kingdom,
    safe_cast(legal_marital_status_2 as string) legal_marital_status_2,
    safe_cast(children_living_at_home as string) children_living_at_home,
    safe_cast(domicile_type as string) domicile_type,
    safe_cast(
        accommodation_problem_mould_rot_windows_doors as string
    ) accommodation_problem_mould_rot_windows_doors,
    safe_cast(
        accommodation_problem_damp_walls_leaking_roof as string
    ) accommodation_problem_damp_walls_leaking_roof,
    safe_cast(
        accommodation_problem_lack_indoor_flushing_toilet as string
    ) accommodation_problem_lack_indoor_flushing_toilet,
    safe_cast(
        accommodation_problem_neither_bath_nor_shower as string
    ) accommodation_problem_neither_bath_nor_shower,
    safe_cast(
        accommodation_problem_overcrowding as string
    ) accommodation_problem_overcrowding,
    safe_cast(
        accommodation_problem_extremely_hot_extremely_cold as string
    ) accommodation_problem_extremely_hot_extremely_cold,
    safe_cast(accommodation_problem_noise as string) accommodation_problem_noise,
    safe_cast(
        accommodation_problem_presence_insects_rodents as string
    ) accommodation_problem_presence_insects_rodents,
    safe_cast(
        accommodation_problem_none_these as string
    ) accommodation_problem_none_these,
    safe_cast(accommodation_problem_refusal as string) accommodation_problem_refusal,
    safe_cast(
        accommodation_problem_dont_know as string
    ) accommodation_problem_dont_know,
    safe_cast(
        accommodation_problem_no_answer as string
    ) accommodation_problem_no_answer,
    safe_cast(highest_education as string) highest_education,
    safe_cast(highest_education_es_isced as string) highest_education_es_isced,
    safe_cast(highest_education_austria as string) highest_education_austria,
    safe_cast(highest_education_belgium as string) highest_education_belgium,
    safe_cast(highest_education_bulgaria as string) highest_education_bulgaria,
    safe_cast(highest_education_croatia as string) highest_education_croatia,
    safe_cast(highest_education_cyprus as string) highest_education_cyprus,
    safe_cast(highest_education_estonia as string) highest_education_estonia,
    safe_cast(highest_education_finland as string) highest_education_finland,
    safe_cast(highest_education_france as string) highest_education_france,
    safe_cast(highest_education_germany as string) highest_education_germany,
    safe_cast(highest_education_germany_2 as int64) highest_education_germany_2,
    safe_cast(highest_education_greece as string) highest_education_greece,
    safe_cast(highest_education_hungary as string) highest_education_hungary,
    safe_cast(highest_education_iceland as string) highest_education_iceland,
    safe_cast(highest_education_ireland as string) highest_education_ireland,
    safe_cast(
        highest_education_israeli_education_israel as string
    ) highest_education_israeli_education_israel,
    safe_cast(
        highest_education_russian_education_israel as string
    ) highest_education_russian_education_israel,
    safe_cast(highest_education_italy as string) highest_education_italy,
    safe_cast(highest_education_latvia as string) highest_education_latvia,
    safe_cast(highest_education_lithuania as string) highest_education_lithuania,
    safe_cast(highest_education_montenegro as string) highest_education_montenegro,
    safe_cast(highest_education_netherlands as string) highest_education_netherlands,
    safe_cast(highest_education_norway as string) highest_education_norway,
    safe_cast(highest_education_poland as string) highest_education_poland,
    safe_cast(highest_education_portugal as string) highest_education_portugal,
    safe_cast(highest_education_serbia as string) highest_education_serbia,
    safe_cast(highest_education_slovakia as string) highest_education_slovakia,
    safe_cast(highest_education_slovenia as string) highest_education_slovenia,
    safe_cast(highest_education_spain as string) highest_education_spain,
    safe_cast(highest_education_sweden as string) highest_education_sweden,
    safe_cast(highest_education_switzerland as string) highest_education_switzerland,
    safe_cast(highest_education_ukraine as string) highest_education_ukraine,
    safe_cast(
        highest_education_united_kingdom as string
    ) highest_education_united_kingdom,
    safe_cast(
        highest_education_united_kingdom_2 as string
    ) highest_education_united_kingdom_2,
    safe_cast(
        age_when_completed_full_time_education as int64
    ) age_when_completed_full_time_education,
    safe_cast(years_of_education as int64) years_of_education,
    safe_cast(paid_work_last_week as string) paid_work_last_week,
    safe_cast(in_education_last_week as string) in_education_last_week,
    safe_cast(unemployed_active_last_week as string) unemployed_active_last_week,
    safe_cast(unemployed_inactive_last_week as string) unemployed_inactive_last_week,
    safe_cast(permanently_disabled_last_week as string) permanently_disabled_last_week,
    safe_cast(retired_last_week as string) retired_last_week,
    safe_cast(
        activity_community_military_service as string
    ) activity_community_military_service,
    safe_cast(housework_last_week as string) housework_last_week,
    safe_cast(activity_other as string) activity_other,
    safe_cast(activity_refusal as string) activity_refusal,
    safe_cast(activity_dont_know as string) activity_dont_know,
    safe_cast(activity_no_answer as string) activity_no_answer,
    safe_cast(main_activity_last_7_days as string) main_activity_last_7_days,
    safe_cast(
        main_activity_7_days_all_respondent_post_coded as string
    ) main_activity_7_days_all_respondent_post_coded,
    safe_cast(control_paid_work_7_days as string) control_paid_work_7_days,
    safe_cast(ever_had_paid_job as string) ever_had_paid_job,
    safe_cast(year_paid_job as int64) year_paid_job,
    safe_cast(employment_relation as string) employment_relation,
    safe_cast(number_of_employees as int64) number_of_employees,
    safe_cast(employment_contract_type as string) employment_contract_type,
    safe_cast(establishment_size as string) establishment_size,
    safe_cast(
        responsible_supervising_other_employees as string
    ) responsible_supervising_other_employees,
    safe_cast(number_people_responsible_job as int64) number_people_responsible_job,
    safe_cast(
        allowed_decide_daily_work_organised as int64
    ) allowed_decide_daily_work_organised,
    safe_cast(
        allowed_influence_policy_decisions_activities_organisation as int64
    ) allowed_influence_policy_decisions_activities_organisation,
    safe_cast(contracted_weekly_hours as int64) contracted_weekly_hours,
    safe_cast(total_weekly_hours as int64) total_weekly_hours,
    safe_cast(industry_nace_rev_2 as string) industry_nace_rev_2,
    safe_cast(
        what_type_organisation_work_worked as string
    ) what_type_organisation_work_worked,
    safe_cast(occupation_isco08 as string) occupation_isco08,
    safe_cast(
        paid_work_another_country_period_more as string
    ) paid_work_another_country_period_more,
    safe_cast(
        ever_unemployed_seeking_work_period_more as string
    ) ever_unemployed_seeking_work_period_more,
    safe_cast(
        any_period_unemployment_work_seeking_lasted as string
    ) any_period_unemployment_work_seeking_lasted,
    safe_cast(
        any_period_unemployment_work_seeking_within as string
    ) any_period_unemployment_work_seeking_within,
    safe_cast(
        member_trade_union_similar_organisation as string
    ) member_trade_union_similar_organisation,
    safe_cast(main_source_household_income as string) main_source_household_income,
    safe_cast(household_income_decile as string) household_income_decile,
    safe_cast(feeling_about_household_income as string) feeling_about_household_income,
    safe_cast(partner_highest_education as string) partner_highest_education,
    safe_cast(
        partner_highest_education_es_isced as string
    ) partner_highest_education_es_isced,
    safe_cast(
        partner_highest_education_austria as string
    ) partner_highest_education_austria,
    safe_cast(
        partner_highest_education_belgium as string
    ) partner_highest_education_belgium,
    safe_cast(
        partner_highest_education_bulgaria as string
    ) partner_highest_education_bulgaria,
    safe_cast(
        partner_highest_education_croatia as string
    ) partner_highest_education_croatia,
    safe_cast(
        partner_highest_education_cyprus as string
    ) partner_highest_education_cyprus,
    safe_cast(
        partner_highest_education_estonia as string
    ) partner_highest_education_estonia,
    safe_cast(
        partner_highest_education_finland as string
    ) partner_highest_education_finland,
    safe_cast(
        partner_highest_education_france as string
    ) partner_highest_education_france,
    safe_cast(
        partner_highest_education_germany as string
    ) partner_highest_education_germany,
    safe_cast(
        partner_highest_education_germany_2 as string
    ) partner_highest_education_germany_2,
    safe_cast(
        partner_highest_education_greece as string
    ) partner_highest_education_greece,
    safe_cast(
        partner_highest_education_hungary as string
    ) partner_highest_education_hungary,
    safe_cast(
        partner_highest_education_iceland as string
    ) partner_highest_education_iceland,
    safe_cast(
        partner_highest_education_ireland as string
    ) partner_highest_education_ireland,
    safe_cast(
        partner_highest_education_russian_education_israel as string
    ) partner_highest_education_russian_education_israel,
    safe_cast(
        partner_highest_education_israeli_education_israel as string
    ) partner_highest_education_israeli_education_israel,
    safe_cast(
        partner_highest_education_italy as string
    ) partner_highest_education_italy,
    safe_cast(
        partner_highest_education_latvia as string
    ) partner_highest_education_latvia,
    safe_cast(
        partner_highest_education_lithuania as string
    ) partner_highest_education_lithuania,
    safe_cast(
        partner_highest_education_montenegro as string
    ) partner_highest_education_montenegro,
    safe_cast(
        partner_highest_education_netherlands as string
    ) partner_highest_education_netherlands,
    safe_cast(
        partner_highest_education_norway as string
    ) partner_highest_education_norway,
    safe_cast(
        partner_highest_education_poland as string
    ) partner_highest_education_poland,
    safe_cast(
        partner_highest_education_portugal as int64
    ) partner_highest_education_portugal,
    safe_cast(
        partner_highest_education_serbia as string
    ) partner_highest_education_serbia,
    safe_cast(
        partner_highest_education_slovakia as string
    ) partner_highest_education_slovakia,
    safe_cast(
        partner_highest_education_slovenia as string
    ) partner_highest_education_slovenia,
    safe_cast(
        partner_highest_education_spain as string
    ) partner_highest_education_spain,
    safe_cast(
        partner_highest_education_sweden as string
    ) partner_highest_education_sweden,
    safe_cast(
        partner_highest_education_switzerland as string
    ) partner_highest_education_switzerland,
    safe_cast(
        partner_highest_education_ukraine as string
    ) partner_highest_education_ukraine,
    safe_cast(
        partner_highest_education_united_kingdom as string
    ) partner_highest_education_united_kingdom,
    safe_cast(
        partner_highest_education_united_kingdom_2 as string
    ) partner_highest_education_united_kingdom_2,
    safe_cast(
        partner_age_when_completed_full_time as int64
    ) partner_age_when_completed_full_time,
    safe_cast(partner_activity_paid_work as string) partner_activity_paid_work,
    safe_cast(partner_activity_education as string) partner_activity_education,
    safe_cast(
        partner_activity_unemployed_actively_looking_job as string
    ) partner_activity_unemployed_actively_looking_job,
    safe_cast(
        partner_activity_unemployed_not_actively_looking as string
    ) partner_activity_unemployed_not_actively_looking,
    safe_cast(
        partner_activity_permanently_sick_disabled as string
    ) partner_activity_permanently_sick_disabled,
    safe_cast(partner_activity_retired as string) partner_activity_retired,
    safe_cast(
        partner_activity_community_military_service as string
    ) partner_activity_community_military_service,
    safe_cast(
        partner_activity_housework_looking_after_children as string
    ) partner_activity_housework_looking_after_children,
    safe_cast(partner_activity_other as string) partner_activity_other,
    safe_cast(partner_activity_dont_know as string) partner_activity_dont_know,
    safe_cast(
        partner_activity_not_applicable as string
    ) partner_activity_not_applicable,
    safe_cast(partner_activity_refusal as string) partner_activity_refusal,
    safe_cast(partner_activity_no_answer as string) partner_activity_no_answer,
    safe_cast(partner_main_activity_7_days as string) partner_main_activity_7_days,
    safe_cast(
        partner_control_paid_work_7_days as string
    ) partner_control_paid_work_7_days,
    safe_cast(occupation_partner_isco08 as string) occupation_partner_isco08,
    safe_cast(partner_employment_relation as string) partner_employment_relation,
    safe_cast(
        hours_normally_worked_week_main_job as int64
    ) hours_normally_worked_week_main_job,
    safe_cast(father_highest_education as string) father_highest_education,
    safe_cast(
        father_highest_education_es_isced as string
    ) father_highest_education_es_isced,
    safe_cast(
        father_highest_education_austria as string
    ) father_highest_education_austria,
    safe_cast(
        father_highest_education_belgium as string
    ) father_highest_education_belgium,
    safe_cast(
        father_highest_education_bulgaria as string
    ) father_highest_education_bulgaria,
    safe_cast(
        father_highest_education_croatia as string
    ) father_highest_education_croatia,
    safe_cast(
        father_highest_education_cyprus as string
    ) father_highest_education_cyprus,
    safe_cast(
        father_highest_education_estonia as string
    ) father_highest_education_estonia,
    safe_cast(
        father_highest_education_finland as string
    ) father_highest_education_finland,
    safe_cast(
        father_highest_education_france as string
    ) father_highest_education_france,
    safe_cast(
        father_highest_education_germany as string
    ) father_highest_education_germany,
    safe_cast(
        father_highest_education_germany_2 as string
    ) father_highest_education_germany_2,
    safe_cast(
        father_highest_education_greece as string
    ) father_highest_education_greece,
    safe_cast(
        father_highest_education_hungary as string
    ) father_highest_education_hungary,
    safe_cast(
        father_highest_education_iceland as string
    ) father_highest_education_iceland,
    safe_cast(
        father_highest_education_ireland as string
    ) father_highest_education_ireland,
    safe_cast(
        father_highest_education_israeli_education_israel as string
    ) father_highest_education_israeli_education_israel,
    safe_cast(
        father_highest_education_russian_education_israel as string
    ) father_highest_education_russian_education_israel,
    safe_cast(father_highest_education_italy as string) father_highest_education_italy,
    safe_cast(
        father_highest_education_latvia as string
    ) father_highest_education_latvia,
    safe_cast(
        father_highest_education_lithuania as string
    ) father_highest_education_lithuania,
    safe_cast(
        father_highest_education_montenegro as string
    ) father_highest_education_montenegro,
    safe_cast(
        father_highest_education_netherlands as string
    ) father_highest_education_netherlands,
    safe_cast(
        father_highest_education_norway as string
    ) father_highest_education_norway,
    safe_cast(
        father_highest_education_poland as string
    ) father_highest_education_poland,
    safe_cast(
        father_highest_education_portugal as string
    ) father_highest_education_portugal,
    safe_cast(
        father_highest_education_serbia as string
    ) father_highest_education_serbia,
    safe_cast(
        father_highest_education_slovakia as string
    ) father_highest_education_slovakia,
    safe_cast(
        father_highest_education_slovenia as string
    ) father_highest_education_slovenia,
    safe_cast(father_highest_education_spain as string) father_highest_education_spain,
    safe_cast(
        father_highest_education_sweden as string
    ) father_highest_education_sweden,
    safe_cast(
        father_highest_education_switzerland as string
    ) father_highest_education_switzerland,
    safe_cast(
        father_highest_education_ukraine as string
    ) father_highest_education_ukraine,
    safe_cast(
        father_highest_education_united_kingdom as string
    ) father_highest_education_united_kingdom,
    safe_cast(
        father_highest_education_united_kingdom_2 as string
    ) father_highest_education_united_kingdom_2,
    safe_cast(
        father_age_when_completed_full_time as int64
    ) father_age_when_completed_full_time,
    safe_cast(
        father_employment_status_when_respondent_14 as string
    ) father_employment_status_when_respondent_14,
    safe_cast(
        father_occupation_when_respondent_14 as string
    ) father_occupation_when_respondent_14,
    safe_cast(mother_highest_education as string) mother_highest_education,
    safe_cast(
        mother_highest_education_es_isced as string
    ) mother_highest_education_es_isced,
    safe_cast(
        mother_highest_education_austria as string
    ) mother_highest_education_austria,
    safe_cast(
        mother_highest_education_belgium as string
    ) mother_highest_education_belgium,
    safe_cast(
        mother_highest_education_bulgaria as string
    ) mother_highest_education_bulgaria,
    safe_cast(
        mother_highest_education_croatia as string
    ) mother_highest_education_croatia,
    safe_cast(
        mother_highest_education_cyprus as string
    ) mother_highest_education_cyprus,
    safe_cast(
        mother_highest_education_estonia as string
    ) mother_highest_education_estonia,
    safe_cast(
        mother_highest_education_finland as string
    ) mother_highest_education_finland,
    safe_cast(
        mother_highest_education_france as string
    ) mother_highest_education_france,
    safe_cast(
        mother_highest_education_germany as string
    ) mother_highest_education_germany,
    safe_cast(
        mother_highest_education_germany_2 as string
    ) mother_highest_education_germany_2,
    safe_cast(
        mother_highest_education_greece as string
    ) mother_highest_education_greece,
    safe_cast(
        mother_highest_education_hungary as string
    ) mother_highest_education_hungary,
    safe_cast(
        mother_highest_education_iceland as string
    ) mother_highest_education_iceland,
    safe_cast(
        mother_highest_education_ireland as string
    ) mother_highest_education_ireland,
    safe_cast(
        mother_highest_education_israeli_education_israel as string
    ) mother_highest_education_israeli_education_israel,
    safe_cast(
        mother_highest_education_russian_education_israel as string
    ) mother_highest_education_russian_education_israel,
    safe_cast(mother_highest_education_italy as string) mother_highest_education_italy,
    safe_cast(
        mother_highest_education_latvia as string
    ) mother_highest_education_latvia,
    safe_cast(
        mother_highest_education_lithuania as string
    ) mother_highest_education_lithuania,
    safe_cast(
        mother_highest_education_montenegro as string
    ) mother_highest_education_montenegro,
    safe_cast(
        mother_highest_education_netherlands as string
    ) mother_highest_education_netherlands,
    safe_cast(
        mother_highest_education_norway as string
    ) mother_highest_education_norway,
    safe_cast(
        mother_highest_education_poland as string
    ) mother_highest_education_poland,
    safe_cast(
        mother_highest_education_portugal as int64
    ) mother_highest_education_portugal,
    safe_cast(
        mother_highest_education_serbia as string
    ) mother_highest_education_serbia,
    safe_cast(
        mother_highest_education_slovakia as string
    ) mother_highest_education_slovakia,
    safe_cast(
        mother_highest_education_slovenia as string
    ) mother_highest_education_slovenia,
    safe_cast(mother_highest_education_spain as string) mother_highest_education_spain,
    safe_cast(
        mother_highest_education_sweden as string
    ) mother_highest_education_sweden,
    safe_cast(
        mother_highest_education_switzerland as string
    ) mother_highest_education_switzerland,
    safe_cast(
        mother_highest_education_ukraine as string
    ) mother_highest_education_ukraine,
    safe_cast(
        mother_highest_education_united_kingdom as string
    ) mother_highest_education_united_kingdom,
    safe_cast(
        mother_highest_education_united_kingdom_2 as string
    ) mother_highest_education_united_kingdom_2,
    safe_cast(
        mother_age_when_completed_full_time as int64
    ) mother_age_when_completed_full_time,
    safe_cast(
        mother_employment_status_when_respondent_14 as string
    ) mother_employment_status_when_respondent_14,
    safe_cast(
        mother_occupation_when_respondent_14 as string
    ) mother_occupation_when_respondent_14,
    safe_cast(
        improve_skills_course_lecture_conference_12 as string
    ) improve_skills_course_lecture_conference_12,
    safe_cast(
        first_ancestry_european_standard_classification_cultural as string
    ) first_ancestry_european_standard_classification_cultural,
    safe_cast(
        second_ancestry_european_standard_classification_cultural as string
    ) second_ancestry_european_standard_classification_cultural,
    safe_cast(regional_unit as string) regional_unit,
    safe_cast(region_code as string) region_code,
    safe_cast(value_creativity as string) value_creativity,
    safe_cast(value_wealth as string) value_wealth,
    safe_cast(value_equality as string) value_equality,
    safe_cast(value_show_abilities as string) value_show_abilities,
    safe_cast(value_safety as string) value_safety,
    safe_cast(value_variety as string) value_variety,
    safe_cast(value_follow_rules as string) value_follow_rules,
    safe_cast(value_understanding_others as string) value_understanding_others,
    safe_cast(value_modesty as string) value_modesty,
    safe_cast(value_good_time as string) value_good_time,
    safe_cast(value_freedom as string) value_freedom,
    safe_cast(value_helping_others as string) value_helping_others,
    safe_cast(value_success as string) value_success,
    safe_cast(value_strong_government as string) value_strong_government,
    safe_cast(value_adventure as string) value_adventure,
    safe_cast(value_behaving_properly as string) value_behaving_properly,
    safe_cast(value_respect_from_others as string) value_respect_from_others,
    safe_cast(value_loyalty_to_friends as string) value_loyalty_to_friends,
    safe_cast(value_environment as string) value_environment,
    safe_cast(value_tradition as string) value_tradition,
    safe_cast(value_fun as string) value_fun,
    safe_cast(
        imagine_large_numbers_people_limit_energy_4 as string
    ) imagine_large_numbers_people_limit_energy_4,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_4 as string
    ) likelihood_large_numbers_people_limit_energy_use_4,
    safe_cast(
        likelihood_governments_enough_countries_take_action_4 as string
    ) likelihood_governments_enough_countries_take_action_4,
    safe_cast(
        imagine_large_numbers_people_limit_energy_5 as string
    ) imagine_large_numbers_people_limit_energy_5,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_5 as string
    ) likelihood_large_numbers_people_limit_energy_use_5,
    safe_cast(
        likelihood_governments_enough_countries_take_action_5 as string
    ) likelihood_governments_enough_countries_take_action_5,
    safe_cast(
        imagine_large_numbers_people_limit_energy_6 as int64
    ) imagine_large_numbers_people_limit_energy_6,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_6 as int64
    ) likelihood_large_numbers_people_limit_energy_use_6,
    safe_cast(
        likelihood_governments_enough_countries_take_action_6 as int64
    ) likelihood_governments_enough_countries_take_action_6,
    safe_cast(respondent_had_coronavirus as string) respondent_had_coronavirus,
    safe_cast(
        respondent_had_symptoms_lasting_3_months_longer as string
    ) respondent_had_symptoms_lasting_3_months_longer,
    safe_cast(respondent_still_has_symptoms as string) respondent_still_has_symptoms,
    safe_cast(
        respondent_received_least_one_dose_vaccine as string
    ) respondent_received_least_one_dose_vaccine,
    safe_cast(agreed_recontacted as string) agreed_recontacted,
    safe_cast(start_interview as string) start_interview,
    safe_cast(start_section as string) start_section,
    safe_cast(end_section as string) end_section,
    safe_cast(end_section_b as string) end_section_b,
    safe_cast(end_section_c as string) end_section_c,
    safe_cast(end_section_d as string) end_section_d,
    safe_cast(end_section_e as string) end_section_e,
    safe_cast(end_section_f as string) end_section_f,
    safe_cast(end_section_h as string) end_section_h,
    safe_cast(end_section_i as string) end_section_i,
    safe_cast(end_section_k as string) end_section_k,
    safe_cast(end_section_r as string) end_section_r,
    safe_cast(end_interview as string) end_interview,
    safe_cast(start_section_j as string) start_section_j,
    safe_cast(end_section_j as string) end_section_j,
    safe_cast(
        interview_length_minutes_main_questionnaire as int64
    ) interview_length_minutes_main_questionnaire,
    safe_cast(mode_data_collection as string) mode_data_collection,
    safe_cast(sampling_domain as int64) sampling_domain,
    safe_cast(selection_probability as float64) selection_probability,
    safe_cast(sampling_stratum as float64) sampling_stratum,
    safe_cast(primary_sampling_unit as float64) primary_sampling_unit
from {{ set_datalake_project("gb_eric_ess_staging.round_11") }} as t
