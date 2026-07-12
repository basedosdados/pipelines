{{
    config(
        schema="gb_eric_ess",
        alias="round_07",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2014, "end": 2019, "interval": 1},
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
    safe_cast(
        tv_watching_total_time_average_weekday as string
    ) tv_watching_total_time_average_weekday,
    safe_cast(
        tv_watching_news_politics_current_affairs as string
    ) tv_watching_news_politics_current_affairs,
    safe_cast(most_people_can_be_trusted as int64) most_people_can_be_trusted,
    safe_cast(most_people_try_to_be_fair as int64) most_people_try_to_be_fair,
    safe_cast(most_people_helpful as int64) most_people_helpful,
    safe_cast(interest_in_politics as string) interest_in_politics,
    safe_cast(
        political_system_allows_people_have_say as int64
    ) political_system_allows_people_have_say,
    safe_cast(
        able_take_active_role_political_group as int64
    ) able_take_active_role_political_group,
    safe_cast(
        political_system_allows_people_have_influence as int64
    ) political_system_allows_people_have_influence,
    safe_cast(
        confident_own_ability_participate_politics as int64
    ) confident_own_ability_participate_politics,
    safe_cast(
        politicians_care_what_people_think as int64
    ) politicians_care_what_people_think,
    safe_cast(easy_take_part_politics as int64) easy_take_part_politics,
    safe_cast(trust_legal_system as int64) trust_legal_system,
    safe_cast(trust_police as int64) trust_police,
    safe_cast(trust_politicians as int64) trust_politicians,
    safe_cast(trust_european_parliament as int64) trust_european_parliament,
    safe_cast(trust_united_nations as int64) trust_united_nations,
    safe_cast(trust_political_parties as int64) trust_political_parties,
    safe_cast(trust_parliament as int64) trust_parliament,
    safe_cast(voted_last_national_election as string) voted_last_national_election,
    safe_cast(party_voted_austria_3 as string) party_voted_austria_3,
    safe_cast(party_voted_belgium_3 as string) party_voted_belgium_3,
    safe_cast(party_voted_switzerland_4 as string) party_voted_switzerland_4,
    safe_cast(party_voted_czechia_2 as string) party_voted_czechia_2,
    safe_cast(party_voted_1_germany_2 as string) party_voted_1_germany_2,
    safe_cast(party_voted_2_germany_2 as string) party_voted_2_germany_2,
    safe_cast(party_voted_denmark_2 as string) party_voted_denmark_2,
    safe_cast(party_voted_estonia_4 as string) party_voted_estonia_4,
    safe_cast(party_voted_spain_4 as string) party_voted_spain_4,
    safe_cast(party_voted_finland_4 as string) party_voted_finland_4,
    safe_cast(party_voted_france_ballot_1_4 as string) party_voted_france_ballot_1_4,
    safe_cast(party_voted_united_kingdom_3 as string) party_voted_united_kingdom_3,
    safe_cast(party_voted_hungary_4 as string) party_voted_hungary_4,
    safe_cast(party_voted_ireland_2 as string) party_voted_ireland_2,
    safe_cast(party_voted_israel_2 as string) party_voted_israel_2,
    safe_cast(
        party_voted_1_lithuania_first_vote_party_3 as string
    ) party_voted_1_lithuania_first_vote_party_3,
    safe_cast(
        party_voted_2_lithuania_second_vote_party_3 as string
    ) party_voted_2_lithuania_second_vote_party_3,
    safe_cast(
        party_voted_3_lithuania_third_vote_party_3 as string
    ) party_voted_3_lithuania_third_vote_party_3,
    safe_cast(party_voted_netherlands_4 as string) party_voted_netherlands_4,
    safe_cast(party_voted_norway_2 as string) party_voted_norway_2,
    safe_cast(party_voted_poland_3 as string) party_voted_poland_3,
    safe_cast(party_voted_portugal_4 as string) party_voted_portugal_4,
    safe_cast(party_voted_sweden_3 as string) party_voted_sweden_3,
    safe_cast(party_voted_slovenia_3 as string) party_voted_slovenia_3,
    safe_cast(contacted_politician as string) contacted_politician,
    safe_cast(
        worked_political_party_action_group_12_months as string
    ) worked_political_party_action_group_12_months,
    safe_cast(
        worked_another_organisation_association_12_months as string
    ) worked_another_organisation_association_12_months,
    safe_cast(worn_campaign_badge as string) worn_campaign_badge,
    safe_cast(signed_petition as string) signed_petition,
    safe_cast(
        taken_part_lawful_public_demonstration_12_months as string
    ) taken_part_lawful_public_demonstration_12_months,
    safe_cast(boycotted_products as string) boycotted_products,
    safe_cast(feel_close_to_party as string) feel_close_to_party,
    safe_cast(party_feel_close_austria_3 as string) party_feel_close_austria_3,
    safe_cast(party_feel_close_belgium_3 as string) party_feel_close_belgium_3,
    safe_cast(party_feel_close_switzerland_4 as string) party_feel_close_switzerland_4,
    safe_cast(party_feel_close_czechia_2 as string) party_feel_close_czechia_2,
    safe_cast(party_feel_close_germany_2 as string) party_feel_close_germany_2,
    safe_cast(party_feel_close_denmark_2 as string) party_feel_close_denmark_2,
    safe_cast(party_feel_close_estonia_4 as string) party_feel_close_estonia_4,
    safe_cast(party_feel_close_spain_4 as string) party_feel_close_spain_4,
    safe_cast(party_feel_close_finland_5 as string) party_feel_close_finland_5,
    safe_cast(party_feel_close_france_4 as string) party_feel_close_france_4,
    safe_cast(
        party_feel_close_united_kingdom_3 as string
    ) party_feel_close_united_kingdom_3,
    safe_cast(party_feel_close_hungary_5 as string) party_feel_close_hungary_5,
    safe_cast(party_feel_close_ireland_4 as string) party_feel_close_ireland_4,
    safe_cast(party_feel_close_israel_2 as string) party_feel_close_israel_2,
    safe_cast(party_feel_close_lithuania_3 as string) party_feel_close_lithuania_3,
    safe_cast(party_feel_close_netherlands_4 as string) party_feel_close_netherlands_4,
    safe_cast(party_feel_close_norway_2 as string) party_feel_close_norway_2,
    safe_cast(party_feel_close_poland_4 as string) party_feel_close_poland_4,
    safe_cast(party_feel_close_portugal_4 as string) party_feel_close_portugal_4,
    safe_cast(party_feel_close_sweden_3 as string) party_feel_close_sweden_3,
    safe_cast(party_feel_close_slovenia_3 as string) party_feel_close_slovenia_3,
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
    safe_cast(european_unification_go_further as int64) european_unification_go_further,
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
    safe_cast(belongs_to_religion as string) belongs_to_religion,
    safe_cast(
        religion_denomination_belonging_present as string
    ) religion_denomination_belonging_present,
    safe_cast(
        religion_denomination_belonging_present_austria as string
    ) religion_denomination_belonging_present_austria,
    safe_cast(
        religion_denomination_belonging_present_belgium as string
    ) religion_denomination_belonging_present_belgium,
    safe_cast(
        religion_denomination_belonging_present_switzerland as string
    ) religion_denomination_belonging_present_switzerland,
    safe_cast(
        religion_denomination_belonging_present_germany as string
    ) religion_denomination_belonging_present_germany,
    safe_cast(
        religion_denomination_belonging_present_finland as string
    ) religion_denomination_belonging_present_finland,
    safe_cast(
        religion_denomination_belonging_present_united_kingdom as string
    ) religion_denomination_belonging_present_united_kingdom,
    safe_cast(
        religion_denomination_belonging_present_hungary as string
    ) religion_denomination_belonging_present_hungary,
    safe_cast(
        religion_denomination_belonging_present_ireland as string
    ) religion_denomination_belonging_present_ireland,
    safe_cast(
        religion_denomination_belonging_present_israel as string
    ) religion_denomination_belonging_present_israel,
    safe_cast(
        religion_denomination_belonging_present_lithuania as string
    ) religion_denomination_belonging_present_lithuania,
    safe_cast(
        religion_denomination_belonging_present_netherlands_2 as string
    ) religion_denomination_belonging_present_netherlands_2,
    safe_cast(
        religion_denomination_belonging_present_norway as string
    ) religion_denomination_belonging_present_norway,
    safe_cast(
        religion_denomination_belonging_present_poland as string
    ) religion_denomination_belonging_present_poland,
    safe_cast(
        religion_denomination_belonging_present_sweden as string
    ) religion_denomination_belonging_present_sweden,
    safe_cast(
        religion_denomination_belonging_present_slovenia as string
    ) religion_denomination_belonging_present_slovenia,
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
        religion_denomination_belonging_past_belgium as string
    ) religion_denomination_belonging_past_belgium,
    safe_cast(
        religion_denomination_belonging_past_switzerland as string
    ) religion_denomination_belonging_past_switzerland,
    safe_cast(
        religion_denomination_belonging_past_germany as string
    ) religion_denomination_belonging_past_germany,
    safe_cast(
        religion_denomination_belonging_past_finland as string
    ) religion_denomination_belonging_past_finland,
    safe_cast(
        religion_denomination_belonging_past_united_kingdom as string
    ) religion_denomination_belonging_past_united_kingdom,
    safe_cast(
        religion_denomination_belonging_past_hungary as string
    ) religion_denomination_belonging_past_hungary,
    safe_cast(
        religion_denomination_belonging_past_ireland as string
    ) religion_denomination_belonging_past_ireland,
    safe_cast(
        religion_denomination_belonging_past_israel as string
    ) religion_denomination_belonging_past_israel,
    safe_cast(
        religion_denomination_belonging_past_lithuania as string
    ) religion_denomination_belonging_past_lithuania,
    safe_cast(
        religion_denomination_belonging_past_netherlands_2 as string
    ) religion_denomination_belonging_past_netherlands_2,
    safe_cast(
        religion_denomination_belonging_past_norway as string
    ) religion_denomination_belonging_past_norway,
    safe_cast(
        religion_denomination_belonging_past_poland as string
    ) religion_denomination_belonging_past_poland,
    safe_cast(
        religion_denomination_belonging_past_sweden as string
    ) religion_denomination_belonging_past_sweden,
    safe_cast(
        religion_denomination_belonging_past_slovenia as string
    ) religion_denomination_belonging_past_slovenia,
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
    safe_cast(citizenship_2 as string) citizenship_2,
    safe_cast(born_in_country as string) born_in_country,
    safe_cast(country_birth_2 as string) country_birth_2,
    safe_cast(year_arrived_country as int64) year_arrived_country,
    safe_cast(language_home_first as string) language_home_first,
    safe_cast(language_home_second as string) language_home_second,
    safe_cast(belong_ethnic_minority as string) belong_ethnic_minority,
    safe_cast(father_born_in_country as string) father_born_in_country,
    safe_cast(country_birth_father_2 as string) country_birth_father_2,
    safe_cast(mother_born_in_country as string) mother_born_in_country,
    safe_cast(country_birth_mother_2 as string) country_birth_mother_2,
    safe_cast(month_birth as string) month_birth,
    safe_cast(
        allow_many_few_immigrants_from_poorer as string
    ) allow_many_few_immigrants_from_poorer,
    safe_cast(
        people_minority_race_ethnic_group_current as string
    ) people_minority_race_ethnic_group_current,
    safe_cast(
        every_100_people_country_number_born as string
    ) every_100_people_country_number_born,
    safe_cast(
        qualification_immigration_good_educational_qualifications as int64
    ) qualification_immigration_good_educational_qualifications,
    safe_cast(
        qualification_immigration_speak_country_official_language as int64
    ) qualification_immigration_speak_country_official_language,
    safe_cast(
        qualification_immigration_christian_background as int64
    ) qualification_immigration_christian_background,
    safe_cast(qualification_immigration_white as int64) qualification_immigration_white,
    safe_cast(
        qualification_immigration_work_skills_needed_country as int64
    ) qualification_immigration_work_skills_needed_country,
    safe_cast(
        qualification_immigration_committed_way_life_country as int64
    ) qualification_immigration_committed_way_life_country,
    safe_cast(
        immigrants_take_jobs_away_country_create as int64
    ) immigrants_take_jobs_away_country_create,
    safe_cast(
        taxes_services_immigrants_take_out_more as int64
    ) taxes_services_immigrants_take_out_more,
    safe_cast(
        immigrants_make_country_crime_problems_worse as int64
    ) immigrants_make_country_crime_problems_worse,
    safe_cast(immigrant_different_race_boss as int64) immigrant_different_race_boss,
    safe_cast(
        immigrant_different_race_married_close_relative as int64
    ) immigrant_different_race_married_close_relative,
    safe_cast(
        better_country_if_almost_everyone_shares as string
    ) better_country_if_almost_everyone_shares,
    safe_cast(
        law_against_ethnic_discrimination_workplace_good as int64
    ) law_against_ethnic_discrimination_workplace_good,
    safe_cast(
        government_should_generous_judging_applications_refugee as string
    ) government_should_generous_judging_applications_refugee,
    safe_cast(interviewer_code_born_country as string) interviewer_code_born_country,
    safe_cast(
        compared_yourself_government_treats_new_immigrants as string
    ) compared_yourself_government_treats_new_immigrants,
    safe_cast(
        religious_beliefs_practices_undermined_enriched_immigrants as int64
    ) religious_beliefs_practices_undermined_enriched_immigrants,
    safe_cast(
        different_race_ethnic_have_any_close_friends as string
    ) different_race_ethnic_have_any_close_friends,
    safe_cast(
        different_race_ethnic_contact_frequency as string
    ) different_race_ethnic_contact_frequency,
    safe_cast(
        different_race_ethnic_contact_bad_good as int64
    ) different_race_ethnic_contact_bad_good,
    safe_cast(feel_close_country as string) feel_close_country,
    safe_cast(
        some_races_ethnic_born_less_intelligent as string
    ) some_races_ethnic_born_less_intelligent,
    safe_cast(
        some_races_ethnic_born_harder_working as string
    ) some_races_ethnic_born_harder_working,
    safe_cast(
        some_cultures_much_better_all_equal as string
    ) some_cultures_much_better_all_equal,
    safe_cast(
        allow_many_few_jewish_people_come_live_country as string
    ) allow_many_few_jewish_people_come_live_country,
    safe_cast(
        allow_many_few_muslims_come_live_country as string
    ) allow_many_few_muslims_come_live_country,
    safe_cast(
        allow_many_few_gypsies_come_live_country as string
    ) allow_many_few_gypsies_come_live_country,
    safe_cast(
        administration_attitudes_migrants as string
    ) administration_attitudes_migrants,
    safe_cast(allow_professionals_from as string) allow_professionals_from,
    safe_cast(allow_professionals_from_2 as string) allow_professionals_from_2,
    safe_cast(allow_unskilled_labourers_from as string) allow_unskilled_labourers_from,
    safe_cast(
        allow_unskilled_labourers_from_2 as string
    ) allow_unskilled_labourers_from_2,
    safe_cast(
        frequency_eat_fruit_excluding_drinking_juice as string
    ) frequency_eat_fruit_excluding_drinking_juice,
    safe_cast(
        frequency_eat_vegetables_salad_excluding_potatoes as string
    ) frequency_eat_vegetables_salad_excluding_potatoes,
    safe_cast(
        do_sports_other_physical_activity_number_7_days as int64
    ) do_sports_other_physical_activity_number_7_days,
    safe_cast(cigarettes_smoking_behaviour as string) cigarettes_smoking_behaviour,
    safe_cast(
        number_cigarettes_smoke_typical_day as string
    ) number_cigarettes_smoke_typical_day,
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
    safe_cast(weight_respondent_kg_2 as string) weight_respondent_kg_2,
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
        serious_conflict_between_people_household_when as string
    ) serious_conflict_between_people_household_when,
    safe_cast(
        severe_financial_difficulties_family_when_growing as string
    ) severe_financial_difficulties_family_when_growing,
    safe_cast(
        any_problems_accommodation_listed_showcard as string
    ) any_problems_accommodation_listed_showcard,
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
    safe_cast(household_member_14_gender as string) household_member_14_gender,
    safe_cast(year_of_birth as int64) year_of_birth,
    safe_cast(age as int64) age,
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
        household_member_14_year_of_birth as int64
    ) household_member_14_year_of_birth,
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
        household_member_14_relationship as string
    ) household_member_14_relationship,
    safe_cast(
        interviewer_code_lives_husband_wife_partner as string
    ) interviewer_code_lives_husband_wife_partner,
    safe_cast(
        relationship_husband_wife_partner_currently_living as string
    ) relationship_husband_wife_partner_currently_living,
    safe_cast(
        relationship_husband_wife_partner_currently_living_3 as string
    ) relationship_husband_wife_partner_currently_living_3,
    safe_cast(
        ever_lived_partner_without_married as string
    ) ever_lived_partner_without_married,
    safe_cast(
        ever_divorced_had_civil_union_dissolved as string
    ) ever_divorced_had_civil_union_dissolved,
    safe_cast(
        interviewer_code_lives_husband_wife_partner_2 as string
    ) interviewer_code_lives_husband_wife_partner_2,
    safe_cast(
        interviewer_code_respondent_cohabiting as string
    ) interviewer_code_respondent_cohabiting,
    safe_cast(legal_marital_status as string) legal_marital_status,
    safe_cast(legal_marital_status_finland as string) legal_marital_status_finland,
    safe_cast(legal_marital_status_ireland as string) legal_marital_status_ireland,
    safe_cast(legal_marital_status_2 as string) legal_marital_status_2,
    safe_cast(children_living_home_not as string) children_living_home_not,
    safe_cast(children_living_at_home as string) children_living_at_home,
    safe_cast(domicile_type as string) domicile_type,
    safe_cast(highest_education_es_isced as string) highest_education_es_isced,
    safe_cast(highest_education as string) highest_education,
    safe_cast(highest_education_austria as string) highest_education_austria,
    safe_cast(highest_education_belgium as string) highest_education_belgium,
    safe_cast(highest_education_switzerland as string) highest_education_switzerland,
    safe_cast(highest_education_czechia as string) highest_education_czechia,
    safe_cast(highest_education_germany_6 as string) highest_education_germany_6,
    safe_cast(highest_education_germany_7 as string) highest_education_germany_7,
    safe_cast(highest_education_germany_8 as string) highest_education_germany_8,
    safe_cast(highest_education_denmark as string) highest_education_denmark,
    safe_cast(highest_education_estonia as string) highest_education_estonia,
    safe_cast(highest_education_spain_2 as string) highest_education_spain_2,
    safe_cast(highest_education_finland as string) highest_education_finland,
    safe_cast(highest_education_france as string) highest_education_france,
    safe_cast(
        highest_education_united_kingdom_3 as string
    ) highest_education_united_kingdom_3,
    safe_cast(
        highest_education_united_kingdom_4 as string
    ) highest_education_united_kingdom_4,
    safe_cast(
        age_when_completed_full_time_education as int64
    ) age_when_completed_full_time_education,
    safe_cast(highest_education_hungary_2 as string) highest_education_hungary_2,
    safe_cast(highest_education_ireland as string) highest_education_ireland,
    safe_cast(
        highest_education_israeli_education_israel_2 as string
    ) highest_education_israeli_education_israel_2,
    safe_cast(
        highest_education_russian_education_israel as string
    ) highest_education_russian_education_israel,
    safe_cast(highest_education_lithuania as string) highest_education_lithuania,
    safe_cast(highest_education_netherlands as string) highest_education_netherlands,
    safe_cast(highest_education_norway_2 as string) highest_education_norway_2,
    safe_cast(highest_education_poland_3 as string) highest_education_poland_3,
    safe_cast(
        tertiary_education_lower_higher_single_tier as string
    ) tertiary_education_lower_higher_single_tier,
    safe_cast(highest_education_portugal_2 as string) highest_education_portugal_2,
    safe_cast(highest_education_sweden as string) highest_education_sweden,
    safe_cast(highest_education_slovenia_2 as string) highest_education_slovenia_2,
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
    safe_cast(
        interviewer_code_one_more_than_one_doing_7_days as string
    ) interviewer_code_one_more_than_one_doing_7_days,
    safe_cast(main_activity_last_7_days as string) main_activity_last_7_days,
    safe_cast(
        main_activity_7_days_all_respondent_post_coded as string
    ) main_activity_7_days_all_respondent_post_coded,
    safe_cast(
        interviewer_code_respondent_paid_work as string
    ) interviewer_code_respondent_paid_work,
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
    safe_cast(
        have_set_basic_contracted_number_hours as string
    ) have_set_basic_contracted_number_hours,
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
    safe_cast(
        interviewer_code_lives_husband_wife_partner_3 as string
    ) interviewer_code_lives_husband_wife_partner_3,
    safe_cast(
        partner_highest_education_es_isced as string
    ) partner_highest_education_es_isced,
    safe_cast(partner_highest_education as string) partner_highest_education,
    safe_cast(
        partner_highest_education_austria_2 as string
    ) partner_highest_education_austria_2,
    safe_cast(
        partner_highest_education_belgium as string
    ) partner_highest_education_belgium,
    safe_cast(
        partner_highest_education_switzerland as string
    ) partner_highest_education_switzerland,
    safe_cast(
        partner_highest_education_czechia as string
    ) partner_highest_education_czechia,
    safe_cast(
        partner_highest_education_germany_6 as string
    ) partner_highest_education_germany_6,
    safe_cast(
        partner_highest_education_germany_7 as string
    ) partner_highest_education_germany_7,
    safe_cast(
        partner_highest_education_germany_8 as string
    ) partner_highest_education_germany_8,
    safe_cast(
        partner_highest_education_denmark as string
    ) partner_highest_education_denmark,
    safe_cast(
        partner_highest_education_estonia as string
    ) partner_highest_education_estonia,
    safe_cast(
        partner_highest_education_spain_2 as string
    ) partner_highest_education_spain_2,
    safe_cast(
        partner_highest_education_finland as string
    ) partner_highest_education_finland,
    safe_cast(
        partner_highest_education_france as string
    ) partner_highest_education_france,
    safe_cast(
        partner_highest_education_united_kingdom_3 as string
    ) partner_highest_education_united_kingdom_3,
    safe_cast(
        partner_highest_education_united_kingdom_4 as string
    ) partner_highest_education_united_kingdom_4,
    safe_cast(
        partner_age_when_completed_full_time as int64
    ) partner_age_when_completed_full_time,
    safe_cast(
        partner_highest_education_hungary_3 as string
    ) partner_highest_education_hungary_3,
    safe_cast(
        partner_highest_education_ireland as string
    ) partner_highest_education_ireland,
    safe_cast(
        partner_highest_education_israeli_education_israel_2 as string
    ) partner_highest_education_israeli_education_israel_2,
    safe_cast(
        partner_highest_education_russian_education_israel as string
    ) partner_highest_education_russian_education_israel,
    safe_cast(
        partner_highest_education_lithuania as string
    ) partner_highest_education_lithuania,
    safe_cast(
        partner_highest_education_netherlands as string
    ) partner_highest_education_netherlands,
    safe_cast(
        partner_highest_education_norway_2 as string
    ) partner_highest_education_norway_2,
    safe_cast(
        partner_highest_education_poland_3 as string
    ) partner_highest_education_poland_3,
    safe_cast(
        partner_tertiary_education_lower_higher_single as string
    ) partner_tertiary_education_lower_higher_single,
    safe_cast(
        partner_highest_education_portugal_2 as string
    ) partner_highest_education_portugal_2,
    safe_cast(
        partner_highest_education_sweden as string
    ) partner_highest_education_sweden,
    safe_cast(
        partner_highest_education_slovenia_2 as string
    ) partner_highest_education_slovenia_2,
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
    safe_cast(
        partner_activity_not_applicable as string
    ) partner_activity_not_applicable,
    safe_cast(partner_activity_refusal as string) partner_activity_refusal,
    safe_cast(partner_activity_dont_know as string) partner_activity_dont_know,
    safe_cast(partner_activity_no_answer as string) partner_activity_no_answer,
    safe_cast(
        interviewer_code_one_more_than_one as string
    ) interviewer_code_one_more_than_one,
    safe_cast(partner_main_activity_7_days as string) partner_main_activity_7_days,
    safe_cast(
        interviewer_code_respondent_partner_paid_work as string
    ) interviewer_code_respondent_partner_paid_work,
    safe_cast(
        partner_control_paid_work_7_days as string
    ) partner_control_paid_work_7_days,
    safe_cast(occupation_partner_isco08 as string) occupation_partner_isco08,
    safe_cast(partner_employment_relation as string) partner_employment_relation,
    safe_cast(
        hours_normally_worked_week_main_job as int64
    ) hours_normally_worked_week_main_job,
    safe_cast(
        father_highest_education_es_isced as string
    ) father_highest_education_es_isced,
    safe_cast(father_highest_education as string) father_highest_education,
    safe_cast(
        father_highest_education_austria as string
    ) father_highest_education_austria,
    safe_cast(
        father_highest_education_belgium as string
    ) father_highest_education_belgium,
    safe_cast(
        father_highest_education_switzerland as string
    ) father_highest_education_switzerland,
    safe_cast(
        father_highest_education_czechia as string
    ) father_highest_education_czechia,
    safe_cast(
        father_highest_education_germany_6 as string
    ) father_highest_education_germany_6,
    safe_cast(
        father_highest_education_germany_7 as string
    ) father_highest_education_germany_7,
    safe_cast(
        father_highest_education_germany_8 as string
    ) father_highest_education_germany_8,
    safe_cast(
        father_highest_education_denmark as string
    ) father_highest_education_denmark,
    safe_cast(
        father_highest_education_estonia as string
    ) father_highest_education_estonia,
    safe_cast(
        father_highest_education_spain_2 as string
    ) father_highest_education_spain_2,
    safe_cast(
        father_highest_education_finland as string
    ) father_highest_education_finland,
    safe_cast(
        father_highest_education_france as string
    ) father_highest_education_france,
    safe_cast(
        father_highest_education_united_kingdom_3 as string
    ) father_highest_education_united_kingdom_3,
    safe_cast(
        father_highest_education_united_kingdom_4 as string
    ) father_highest_education_united_kingdom_4,
    safe_cast(
        father_age_when_completed_full_time as int64
    ) father_age_when_completed_full_time,
    safe_cast(
        father_highest_education_hungary_3 as string
    ) father_highest_education_hungary_3,
    safe_cast(
        father_highest_education_ireland as string
    ) father_highest_education_ireland,
    safe_cast(
        father_highest_education_israeli_education_israel_2 as string
    ) father_highest_education_israeli_education_israel_2,
    safe_cast(
        father_highest_education_russian_education_israel as string
    ) father_highest_education_russian_education_israel,
    safe_cast(
        father_highest_education_lithuania as string
    ) father_highest_education_lithuania,
    safe_cast(
        father_highest_education_netherlands as string
    ) father_highest_education_netherlands,
    safe_cast(
        father_highest_education_norway_2 as string
    ) father_highest_education_norway_2,
    safe_cast(
        father_highest_education_poland_3 as string
    ) father_highest_education_poland_3,
    safe_cast(
        father_highest_education_portugal_2 as string
    ) father_highest_education_portugal_2,
    safe_cast(
        father_highest_education_sweden as string
    ) father_highest_education_sweden,
    safe_cast(
        father_highest_education_slovenia_2 as string
    ) father_highest_education_slovenia_2,
    safe_cast(
        father_employment_status_when_respondent_14 as string
    ) father_employment_status_when_respondent_14,
    safe_cast(
        father_occupation_when_respondent_14 as string
    ) father_occupation_when_respondent_14,
    safe_cast(
        mother_highest_education_es_isced as string
    ) mother_highest_education_es_isced,
    safe_cast(mother_highest_education as string) mother_highest_education,
    safe_cast(
        mother_highest_education_austria as string
    ) mother_highest_education_austria,
    safe_cast(
        mother_highest_education_belgium as string
    ) mother_highest_education_belgium,
    safe_cast(
        mother_highest_education_switzerland as string
    ) mother_highest_education_switzerland,
    safe_cast(
        mother_highest_education_czechia as string
    ) mother_highest_education_czechia,
    safe_cast(
        mother_highest_education_germany_6 as string
    ) mother_highest_education_germany_6,
    safe_cast(
        mother_highest_education_germany_7 as string
    ) mother_highest_education_germany_7,
    safe_cast(
        mother_highest_education_germany_8 as string
    ) mother_highest_education_germany_8,
    safe_cast(
        mother_highest_education_denmark as string
    ) mother_highest_education_denmark,
    safe_cast(
        mother_highest_education_estonia as string
    ) mother_highest_education_estonia,
    safe_cast(
        mother_highest_education_spain_2 as string
    ) mother_highest_education_spain_2,
    safe_cast(
        mother_highest_education_finland as string
    ) mother_highest_education_finland,
    safe_cast(
        mother_highest_education_france as string
    ) mother_highest_education_france,
    safe_cast(
        mother_highest_education_united_kingdom_3 as string
    ) mother_highest_education_united_kingdom_3,
    safe_cast(
        mother_highest_education_united_kingdom_4 as string
    ) mother_highest_education_united_kingdom_4,
    safe_cast(
        mother_age_when_completed_full_time as int64
    ) mother_age_when_completed_full_time,
    safe_cast(
        mother_highest_education_hungary_3 as string
    ) mother_highest_education_hungary_3,
    safe_cast(
        mother_highest_education_ireland as string
    ) mother_highest_education_ireland,
    safe_cast(
        mother_highest_education_israeli_education_israel_2 as string
    ) mother_highest_education_israeli_education_israel_2,
    safe_cast(
        mother_highest_education_russian_education_israel as string
    ) mother_highest_education_russian_education_israel,
    safe_cast(
        mother_highest_education_lithuania as string
    ) mother_highest_education_lithuania,
    safe_cast(
        mother_highest_education_netherlands as string
    ) mother_highest_education_netherlands,
    safe_cast(
        mother_highest_education_norway_2 as string
    ) mother_highest_education_norway_2,
    safe_cast(
        mother_highest_education_poland_3 as string
    ) mother_highest_education_poland_3,
    safe_cast(
        mother_highest_education_portugal_2 as string
    ) mother_highest_education_portugal_2,
    safe_cast(
        mother_highest_education_sweden as string
    ) mother_highest_education_sweden,
    safe_cast(
        mother_highest_education_slovenia_2 as string
    ) mother_highest_education_slovenia_2,
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
        first_ancestry_european_standard_classification_cultural_2 as string
    ) first_ancestry_european_standard_classification_cultural_2,
    safe_cast(
        second_ancestry_european_standard_classification_cultural_2 as string
    ) second_ancestry_european_standard_classification_cultural_2,
    safe_cast(region_code as string) region_code,
    safe_cast(regional_unit as string) regional_unit,
    safe_cast(
        place_interview_east_west_germany as string
    ) place_interview_east_west_germany,
    safe_cast(
        important_think_new_ideas_creative as string
    ) important_think_new_ideas_creative,
    safe_cast(
        important_rich_have_money_expensive_things as string
    ) important_rich_have_money_expensive_things,
    safe_cast(
        important_people_treated_equally_have_equal as string
    ) important_people_treated_equally_have_equal,
    safe_cast(
        important_show_abilities_admired as string
    ) important_show_abilities_admired,
    safe_cast(
        important_live_secure_safe_surroundings as string
    ) important_live_secure_safe_surroundings,
    safe_cast(
        important_try_new_different_things_life as string
    ) important_try_new_different_things_life,
    safe_cast(
        important_do_what_told_follow_rules as string
    ) important_do_what_told_follow_rules,
    safe_cast(
        important_understand_different_people as string
    ) important_understand_different_people,
    safe_cast(
        important_humble_modest_not_draw_attention as string
    ) important_humble_modest_not_draw_attention,
    safe_cast(important_have_good_time as string) important_have_good_time,
    safe_cast(
        important_make_own_decisions_free as string
    ) important_make_own_decisions_free,
    safe_cast(
        important_help_people_care_others_well as string
    ) important_help_people_care_others_well,
    safe_cast(
        important_successful_people_recognise_achievements as string
    ) important_successful_people_recognise_achievements,
    safe_cast(
        important_government_strong_ensures_safety as string
    ) important_government_strong_ensures_safety,
    safe_cast(
        important_seek_adventures_have_exciting_life as string
    ) important_seek_adventures_have_exciting_life,
    safe_cast(important_behave_properly as string) important_behave_properly,
    safe_cast(
        important_get_respect_from_others as string
    ) important_get_respect_from_others,
    safe_cast(
        important_loyal_friends_devote_people_close as string
    ) important_loyal_friends_devote_people_close,
    safe_cast(
        important_care_nature_environment as string
    ) important_care_nature_environment,
    safe_cast(
        important_follow_traditions_customs as string
    ) important_follow_traditions_customs,
    safe_cast(
        important_seek_fun_things_give_pleasure as string
    ) important_seek_fun_things_give_pleasure,
    safe_cast(start_interview_day_month as string) start_interview_day_month,
    safe_cast(start_interview_month as string) start_interview_month,
    safe_cast(start_interview_year as string) start_interview_year,
    safe_cast(start_interview_hour as string) start_interview_hour,
    safe_cast(start_interview_minute as string) start_interview_minute,
    safe_cast(end_interview_day_month as string) end_interview_day_month,
    safe_cast(end_interview_month as string) end_interview_month,
    safe_cast(end_interview_year as string) end_interview_year,
    safe_cast(end_interview_hour as string) end_interview_hour,
    safe_cast(end_interview_minute as string) end_interview_minute,
    safe_cast(
        administration_split_ballot_mtmm as string
    ) administration_split_ballot_mtmm,
    safe_cast(
        administration_supplementary_questionnaire_1 as string
    ) administration_supplementary_questionnaire_1,
    safe_cast(
        administration_supplementary_questionnaire_2 as string
    ) administration_supplementary_questionnaire_2,
    safe_cast(
        day_month_supplementary_questionnaire as string
    ) day_month_supplementary_questionnaire,
    safe_cast(
        month_supplementary_questionnaire as string
    ) month_supplementary_questionnaire,
    safe_cast(
        year_supplementary_questionnaire as string
    ) year_supplementary_questionnaire,
    safe_cast(
        interview_length_minutes_main_questionnaire as int64
    ) interview_length_minutes_main_questionnaire
from {{ set_datalake_project("gb_eric_ess_staging.round_07") }} as t
