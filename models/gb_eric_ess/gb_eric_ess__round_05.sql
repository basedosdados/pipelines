{{
    config(
        schema="gb_eric_ess",
        alias="round_05",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2015, "interval": 1},
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
    safe_cast(
        radio_listening_total_time_average_weekday as string
    ) radio_listening_total_time_average_weekday,
    safe_cast(
        radio_listening_news_politics_current_affairs as string
    ) radio_listening_news_politics_current_affairs,
    safe_cast(
        newspaper_reading_total_time_average_weekday as string
    ) newspaper_reading_total_time_average_weekday,
    safe_cast(
        newspaper_reading_politics_current_affairs_average as string
    ) newspaper_reading_politics_current_affairs_average,
    safe_cast(
        personal_use_internet_e_mail_www as string
    ) personal_use_internet_e_mail_www,
    safe_cast(most_people_can_be_trusted as int64) most_people_can_be_trusted,
    safe_cast(most_people_try_to_be_fair as int64) most_people_try_to_be_fair,
    safe_cast(most_people_helpful as int64) most_people_helpful,
    safe_cast(interest_in_politics as string) interest_in_politics,
    safe_cast(trust_legal_system as int64) trust_legal_system,
    safe_cast(trust_police as int64) trust_police,
    safe_cast(trust_politicians as int64) trust_politicians,
    safe_cast(trust_european_parliament as int64) trust_european_parliament,
    safe_cast(trust_united_nations as int64) trust_united_nations,
    safe_cast(trust_political_parties as int64) trust_political_parties,
    safe_cast(trust_parliament as int64) trust_parliament,
    safe_cast(voted_last_national_election as string) voted_last_national_election,
    safe_cast(party_voted_belgium_3 as string) party_voted_belgium_3,
    safe_cast(party_voted_bulgaria_4 as string) party_voted_bulgaria_4,
    safe_cast(party_voted_switzerland_6 as string) party_voted_switzerland_6,
    safe_cast(party_voted_cyprus_4 as string) party_voted_cyprus_4,
    safe_cast(party_voted_czechia_4 as string) party_voted_czechia_4,
    safe_cast(party_voted_1_germany_4 as string) party_voted_1_germany_4,
    safe_cast(party_voted_2_germany_4 as string) party_voted_2_germany_4,
    safe_cast(party_voted_denmark_3 as string) party_voted_denmark_3,
    safe_cast(party_voted_estonia_6 as string) party_voted_estonia_6,
    safe_cast(party_voted_spain_5 as string) party_voted_spain_5,
    safe_cast(party_voted_finland_5 as string) party_voted_finland_5,
    safe_cast(party_voted_france_ballot_1_5 as string) party_voted_france_ballot_1_5,
    safe_cast(party_voted_united_kingdom_4 as string) party_voted_united_kingdom_4,
    safe_cast(party_voted_greece_3 as string) party_voted_greece_3,
    safe_cast(party_voted_croatia_4 as string) party_voted_croatia_4,
    safe_cast(party_voted_hungary_6 as string) party_voted_hungary_6,
    safe_cast(party_voted_ireland_2 as string) party_voted_ireland_2,
    safe_cast(party_voted_israel_3 as string) party_voted_israel_3,
    safe_cast(
        party_voted_1_lithuania_first_vote_party_4 as string
    ) party_voted_1_lithuania_first_vote_party_4,
    safe_cast(
        party_voted_2_lithuania_second_vote_party_4 as string
    ) party_voted_2_lithuania_second_vote_party_4,
    safe_cast(
        party_voted_3_lithuania_third_vote_party_4 as string
    ) party_voted_3_lithuania_third_vote_party_4,
    safe_cast(party_voted_netherlands_6 as string) party_voted_netherlands_6,
    safe_cast(party_voted_norway_3 as string) party_voted_norway_3,
    safe_cast(party_voted_poland_4 as string) party_voted_poland_4,
    safe_cast(party_voted_portugal_4 as string) party_voted_portugal_4,
    safe_cast(
        party_voted_russian_federation_3 as string
    ) party_voted_russian_federation_3,
    safe_cast(party_voted_sweden_4 as string) party_voted_sweden_4,
    safe_cast(party_voted_slovenia_5 as string) party_voted_slovenia_5,
    safe_cast(party_voted_slovakia_4 as string) party_voted_slovakia_4,
    safe_cast(party_voted_ukraine as string) party_voted_ukraine,
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
    safe_cast(party_feel_close_belgium_3 as string) party_feel_close_belgium_3,
    safe_cast(party_feel_close_bulgaria_5 as string) party_feel_close_bulgaria_5,
    safe_cast(party_feel_close_switzerland_6 as string) party_feel_close_switzerland_6,
    safe_cast(party_feel_close_cyprus_4 as string) party_feel_close_cyprus_4,
    safe_cast(party_feel_close_czechia_4 as string) party_feel_close_czechia_4,
    safe_cast(party_feel_close_germany_4 as string) party_feel_close_germany_4,
    safe_cast(party_feel_close_denmark_3 as string) party_feel_close_denmark_3,
    safe_cast(party_feel_close_estonia_6 as string) party_feel_close_estonia_6,
    safe_cast(party_feel_close_spain_6 as string) party_feel_close_spain_6,
    safe_cast(party_feel_close_finland_6 as string) party_feel_close_finland_6,
    safe_cast(party_feel_close_france_5 as string) party_feel_close_france_5,
    safe_cast(
        party_feel_close_united_kingdom_4 as string
    ) party_feel_close_united_kingdom_4,
    safe_cast(party_feel_close_greece_3 as string) party_feel_close_greece_3,
    safe_cast(party_feel_close_croatia_3 as string) party_feel_close_croatia_3,
    safe_cast(party_feel_close_hungary_7 as string) party_feel_close_hungary_7,
    safe_cast(party_feel_close_ireland_4 as string) party_feel_close_ireland_4,
    safe_cast(party_feel_close_israel_4 as string) party_feel_close_israel_4,
    safe_cast(party_feel_close_lithuania_4 as string) party_feel_close_lithuania_4,
    safe_cast(party_feel_close_netherlands_6 as string) party_feel_close_netherlands_6,
    safe_cast(party_feel_close_norway_3 as string) party_feel_close_norway_3,
    safe_cast(party_feel_close_poland_6 as string) party_feel_close_poland_6,
    safe_cast(party_feel_close_portugal_5 as string) party_feel_close_portugal_5,
    safe_cast(
        party_feel_close_russian_federation_3 as string
    ) party_feel_close_russian_federation_3,
    safe_cast(party_feel_close_sweden_4 as string) party_feel_close_sweden_4,
    safe_cast(party_feel_close_slovenia_5 as string) party_feel_close_slovenia_5,
    safe_cast(party_feel_close_slovakia_4 as string) party_feel_close_slovakia_4,
    safe_cast(party_feel_close_ukraine_2 as string) party_feel_close_ukraine_2,
    safe_cast(how_close_to_party as string) how_close_to_party,
    safe_cast(member_political_party as string) member_political_party,
    safe_cast(member_party_belgium as string) member_party_belgium,
    safe_cast(member_party_bulgaria as string) member_party_bulgaria,
    safe_cast(member_party_switzerland as string) member_party_switzerland,
    safe_cast(member_party_cyprus as string) member_party_cyprus,
    safe_cast(member_party_czechia as string) member_party_czechia,
    safe_cast(member_party_germany as string) member_party_germany,
    safe_cast(member_party_denmark as string) member_party_denmark,
    safe_cast(member_party_estonia as string) member_party_estonia,
    safe_cast(member_party_spain as string) member_party_spain,
    safe_cast(member_party_finland as string) member_party_finland,
    safe_cast(member_party_france as string) member_party_france,
    safe_cast(member_party_united_kingdom as string) member_party_united_kingdom,
    safe_cast(member_party_greece as string) member_party_greece,
    safe_cast(member_party_croatia as string) member_party_croatia,
    safe_cast(member_party_hungary as string) member_party_hungary,
    safe_cast(member_party_ireland as string) member_party_ireland,
    safe_cast(member_party_israel as string) member_party_israel,
    safe_cast(member_party_lithuania as string) member_party_lithuania,
    safe_cast(member_party_netherlands as string) member_party_netherlands,
    safe_cast(member_party_norway as string) member_party_norway,
    safe_cast(member_party_poland as string) member_party_poland,
    safe_cast(member_party_portugal as string) member_party_portugal,
    safe_cast(
        member_party_russian_federation as string
    ) member_party_russian_federation,
    safe_cast(member_party_sweden as string) member_party_sweden,
    safe_cast(member_party_slovenia as string) member_party_slovenia,
    safe_cast(member_party_slovakia as string) member_party_slovakia,
    safe_cast(member_party_ukraine as string) member_party_ukraine,
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
    safe_cast(
        ban_political_parties_wish_overthrow_democracy as string
    ) ban_political_parties_wish_overthrow_democracy,
    safe_cast(
        modern_science_can_relied_solve_environmental as string
    ) modern_science_can_relied_solve_environmental,
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
    safe_cast(
        anyone_discuss_intimate_personal_matters as string
    ) anyone_discuss_intimate_personal_matters,
    safe_cast(social_activities_vs_peers as string) social_activities_vs_peers,
    safe_cast(victim_of_crime as string) victim_of_crime,
    safe_cast(feeling_safe_walking_dark as string) feeling_safe_walking_dark,
    safe_cast(frequency_worry_home_burgled as string) frequency_worry_home_burgled,
    safe_cast(
        worry_home_burgled_has_effect_quality_life as string
    ) worry_home_burgled_has_effect_quality_life,
    safe_cast(
        frequency_worry_becoming_victim_violent_crime as string
    ) frequency_worry_becoming_victim_violent_crime,
    safe_cast(
        worry_becoming_victim_violent_crime_has as string
    ) worry_becoming_victim_violent_crime_has,
    safe_cast(subjective_general_health as string) subjective_general_health,
    safe_cast(hampered_by_illness as string) hampered_by_illness,
    safe_cast(belongs_to_religion as string) belongs_to_religion,
    safe_cast(
        religion_denomination_belonging_present as string
    ) religion_denomination_belonging_present,
    safe_cast(
        religion_denomination_belonging_present_belgium as string
    ) religion_denomination_belonging_present_belgium,
    safe_cast(
        religion_denomination_belonging_present_switzerland as string
    ) religion_denomination_belonging_present_switzerland,
    safe_cast(
        religion_denomination_belonging_present_cyprus_2 as string
    ) religion_denomination_belonging_present_cyprus_2,
    safe_cast(
        religion_denomination_belonging_present_germany_2 as string
    ) religion_denomination_belonging_present_germany_2,
    safe_cast(
        religion_denomination_belonging_present_denmark as string
    ) religion_denomination_belonging_present_denmark,
    safe_cast(
        religion_denomination_belonging_present_finland as string
    ) religion_denomination_belonging_present_finland,
    safe_cast(
        religion_denomination_belonging_present_united_kingdom as string
    ) religion_denomination_belonging_present_united_kingdom,
    safe_cast(
        religion_denomination_belonging_present_greece as string
    ) religion_denomination_belonging_present_greece,
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
        religion_denomination_belonging_present_portugal_2 as string
    ) religion_denomination_belonging_present_portugal_2,
    safe_cast(
        religion_denomination_belonging_present_russian_federation as string
    ) religion_denomination_belonging_present_russian_federation,
    safe_cast(
        religion_denomination_belonging_present_sweden as string
    ) religion_denomination_belonging_present_sweden,
    safe_cast(
        religion_denomination_belonging_present_slovenia as string
    ) religion_denomination_belonging_present_slovenia,
    safe_cast(
        religion_denomination_belonging_present_slovakia_3 as string
    ) religion_denomination_belonging_present_slovakia_3,
    safe_cast(
        religion_denomination_belonging_present_ukraine as string
    ) religion_denomination_belonging_present_ukraine,
    safe_cast(
        ever_belonging_particular_religion_denomination as string
    ) ever_belonging_particular_religion_denomination,
    safe_cast(
        religion_denomination_belonging_past as string
    ) religion_denomination_belonging_past,
    safe_cast(
        religion_denomination_belonging_past_belgium as string
    ) religion_denomination_belonging_past_belgium,
    safe_cast(
        religion_denomination_belonging_past_switzerland as string
    ) religion_denomination_belonging_past_switzerland,
    safe_cast(
        religion_denomination_belonging_past_cyprus_2 as string
    ) religion_denomination_belonging_past_cyprus_2,
    safe_cast(
        religion_denomination_belonging_past_germany_2 as string
    ) religion_denomination_belonging_past_germany_2,
    safe_cast(
        religion_denomination_belonging_past_denmark as string
    ) religion_denomination_belonging_past_denmark,
    safe_cast(
        religion_denomination_belonging_past_finland as string
    ) religion_denomination_belonging_past_finland,
    safe_cast(
        religion_denomination_belonging_past_united_kingdom as string
    ) religion_denomination_belonging_past_united_kingdom,
    safe_cast(
        religion_denomination_belonging_past_greece as string
    ) religion_denomination_belonging_past_greece,
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
        religion_denomination_belonging_past_portugal_2 as string
    ) religion_denomination_belonging_past_portugal_2,
    safe_cast(
        religion_denomination_belonging_past_russian_federation as string
    ) religion_denomination_belonging_past_russian_federation,
    safe_cast(
        religion_denomination_belonging_past_sweden as string
    ) religion_denomination_belonging_past_sweden,
    safe_cast(
        religion_denomination_belonging_past_slovenia as string
    ) religion_denomination_belonging_past_slovenia,
    safe_cast(
        religion_denomination_belonging_past_slovakia_3 as string
    ) religion_denomination_belonging_past_slovakia_3,
    safe_cast(
        religion_denomination_belonging_past_ukraine as string
    ) religion_denomination_belonging_past_ukraine,
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
    safe_cast(citizenship_3 as string) citizenship_3,
    safe_cast(born_in_country as string) born_in_country,
    safe_cast(country_birth_3 as string) country_birth_3,
    safe_cast(year_arrived_country as int64) year_arrived_country,
    safe_cast(language_home_first as string) language_home_first,
    safe_cast(language_home_second as string) language_home_second,
    safe_cast(belong_ethnic_minority as string) belong_ethnic_minority,
    safe_cast(father_born_in_country as string) father_born_in_country,
    safe_cast(country_birth_father_3 as string) country_birth_father_3,
    safe_cast(mother_born_in_country as string) mother_born_in_country,
    safe_cast(country_birth_mother_3 as string) country_birth_mother_3,
    safe_cast(
        think_employer_considered_job_temporary_permanent as string
    ) think_employer_considered_job_temporary_permanent,
    safe_cast(main_reason_leaving_employer as string) main_reason_leaving_employer,
    safe_cast(
        proportion_household_income_respondent_provides as string
    ) proportion_household_income_respondent_provides,
    safe_cast(
        number_days_education_training_12_months as string
    ) number_days_education_training_12_months,
    safe_cast(
        useful_would_obtained_knowledge_if_change as string
    ) useful_would_obtained_knowledge_if_change,
    safe_cast(
        much_education_training_paid_employer_firm as string
    ) much_education_training_paid_employer_firm,
    safe_cast(
        have_felt_cheerful_good_spirits_2_weeks as string
    ) have_felt_cheerful_good_spirits_2_weeks,
    safe_cast(have_felt_calm_relaxed_2_weeks as string) have_felt_calm_relaxed_2_weeks,
    safe_cast(
        have_felt_active_vigorous_2_weeks as string
    ) have_felt_active_vigorous_2_weeks,
    safe_cast(
        women_should_prepared_cut_down_paid as string
    ) women_should_prepared_cut_down_paid,
    safe_cast(men_should_have_more_right_job as string) men_should_have_more_right_job,
    safe_cast(
        government_do_more_prevent_people_falling as string
    ) government_do_more_prevent_people_falling,
    safe_cast(
        much_time_during_past_week_felt_lonely as string
    ) much_time_during_past_week_felt_lonely,
    safe_cast(
        what_extent_had_manage_lower_household as string
    ) what_extent_had_manage_lower_household,
    safe_cast(
        what_extent_had_draw_savings_debt as string
    ) what_extent_had_draw_savings_debt,
    safe_cast(
        what_extent_had_cut_back_holidays as string
    ) what_extent_had_cut_back_holidays,
    safe_cast(
        total_number_years_full_part_time_work as string
    ) total_number_years_full_part_time_work,
    safe_cast(
        interviewer_code_main_activity_respondent as string
    ) interviewer_code_main_activity_respondent,
    safe_cast(task_spend_most_time_main_job as string) task_spend_most_time_main_job,
    safe_cast(
        total_years_doing_kind_work_currently as string
    ) total_years_doing_kind_work_currently,
    safe_cast(
        work_involve_working_evenings_nights_frequency as string
    ) work_involve_working_evenings_nights_frequency,
    safe_cast(
        work_involve_having_work_overtime_short as string
    ) work_involve_having_work_overtime_short,
    safe_cast(
        work_involve_working_weekends_frequency as string
    ) work_involve_working_weekends_frequency,
    safe_cast(
        year_first_started_working_current_employer as string
    ) year_first_started_working_current_employer,
    safe_cast(
        know_other_employers_who_would_have as string
    ) know_other_employers_who_would_have,
    safe_cast(
        main_reason_i_put_effort_into_my_work as string
    ) main_reason_i_put_effort_into_my_work,
    safe_cast(
        second_most_important_reason_i_put as string
    ) second_most_important_reason_i_put,
    safe_cast(
        would_someone_applying_job_need_education as string
    ) would_someone_applying_job_need_education,
    safe_cast(
        years_education_beyond_compulsory_needed_applicant as string
    ) years_education_beyond_compulsory_needed_applicant,
    safe_cast(
        somebody_right_qualification_long_learn_do as string
    ) somebody_right_qualification_long_learn_do,
    safe_cast(current_job_variety_work as string) current_job_variety_work,
    safe_cast(
        current_job_job_requires_learning_new as string
    ) current_job_job_requires_learning_new,
    safe_cast(
        current_job_wage_salary_depends_effort as string
    ) current_job_wage_salary_depends_effort,
    safe_cast(
        current_job_can_get_support_help as string
    ) current_job_can_get_support_help,
    safe_cast(
        current_job_health_safety_risk_because as string
    ) current_job_health_safety_risk_because,
    safe_cast(
        current_job_can_decide_time_start_2 as string
    ) current_job_can_decide_time_start_2,
    safe_cast(current_job_job_secure as string) current_job_job_secure,
    safe_cast(
        move_less_interesting_job_organisation_next as string
    ) move_less_interesting_job_organisation_next,
    safe_cast(
        current_job_job_requires_work_very as string
    ) current_job_job_requires_work_very,
    safe_cast(
        current_job_never_enough_time_get as string
    ) current_job_never_enough_time_get,
    safe_cast(
        current_job_good_opportunities_advancement as string
    ) current_job_good_opportunities_advancement,
    safe_cast(
        immediate_supervisor_boss_man_woman as string
    ) immediate_supervisor_boss_man_woman,
    safe_cast(proportion_women_workplace as string) proportion_women_workplace,
    safe_cast(
        difficult_easy_immediate_boss_know_much as int64
    ) difficult_easy_immediate_boss_know_much,
    safe_cast(
        difficult_easy_get_similar_better_job as int64
    ) difficult_easy_get_similar_better_job,
    safe_cast(
        difficult_easy_employer_replace_if_left as int64
    ) difficult_easy_employer_replace_if_left,
    safe_cast(
        regular_meetings_between_representatives_employer_employees as string
    ) regular_meetings_between_representatives_employer_employees,
    safe_cast(
        much_influence_discussions_have_decisions_affect as string
    ) much_influence_discussions_have_decisions_affect,
    safe_cast(
        much_influence_trade_unions_have_over as string
    ) much_influence_trade_unions_have_over,
    safe_cast(
        considering_efforts_achievements_job_i_feel as string
    ) considering_efforts_achievements_job_i_feel,
    safe_cast(
        worry_work_problems_when_not_working_frequency as string
    ) worry_work_problems_when_not_working_frequency,
    safe_cast(
        too_tired_after_work_enjoy_things as string
    ) too_tired_after_work_enjoy_things,
    safe_cast(
        job_prevents_from_giving_time_partner as string
    ) job_prevents_from_giving_time_partner,
    safe_cast(
        interviewer_code_dont_have_partner_family as string
    ) interviewer_code_dont_have_partner_family,
    safe_cast(
        partner_family_fed_up_pressure_job_frequency as string
    ) partner_family_fed_up_pressure_job_frequency,
    safe_cast(
        family_responsibilities_prevent_from_giving_time as string
    ) family_responsibilities_prevent_from_giving_time,
    safe_cast(
        difficult_concentrate_work_because_family_responsibilities as string
    ) difficult_concentrate_work_because_family_responsibilities,
    safe_cast(satisfied_main_job as int64) satisfied_main_job,
    safe_cast(
        satisfied_balance_between_time_job_time as int64
    ) satisfied_balance_between_time_job_time,
    safe_cast(
        i_would_enjoy_working_current_job as string
    ) i_would_enjoy_working_current_job,
    safe_cast(
        usual_gross_pay_euro_before_deductions as string
    ) usual_gross_pay_euro_before_deductions,
    safe_cast(long_period_pay_cover as string) long_period_pay_cover,
    safe_cast(
        had_do_less_interesting_work_3_years as string
    ) had_do_less_interesting_work_3_years,
    safe_cast(had_take_reduction_pay_3_years as string) had_take_reduction_pay_3_years,
    safe_cast(had_work_shorter_hours_3_years as string) had_work_shorter_hours_3_years,
    safe_cast(had_less_security_job_3_years as string) had_less_security_job_3_years,
    safe_cast(
        financial_difficulty_organisation_work_3_years as string
    ) financial_difficulty_organisation_work_3_years,
    safe_cast(
        number_people_employed_organisation_work_3_years as string
    ) number_people_employed_organisation_work_3_years,
    safe_cast(
        interviewer_code_respondent_under_70_years as string
    ) interviewer_code_respondent_under_70_years,
    safe_cast(
        important_if_choosing_job_enabled_use_own as string
    ) important_if_choosing_job_enabled_use_own,
    safe_cast(
        important_if_choosing_secure_job as string
    ) important_if_choosing_secure_job,
    safe_cast(
        important_if_choosing_high_income as string
    ) important_if_choosing_high_income,
    safe_cast(
        important_if_choosing_job_allowed_combine_work as string
    ) important_if_choosing_job_allowed_combine_work,
    safe_cast(
        important_if_choosing_job_offered_good_training as string
    ) important_if_choosing_job_offered_good_training,
    safe_cast(i_would_enjoy_having_paid_job as string) i_would_enjoy_having_paid_job,
    safe_cast(
        longest_period_months_continuously_unemployed_seeking as string
    ) longest_period_months_continuously_unemployed_seeking,
    safe_cast(
        number_hours_would_choose_work_weekly as string
    ) number_hours_would_choose_work_weekly,
    safe_cast(
        interviewer_code_lives_husband_wife_partner_4 as string
    ) interviewer_code_lives_husband_wife_partner_4,
    safe_cast(
        number_hours_week_would_like_partner_work as string
    ) number_hours_week_would_like_partner_work,
    safe_cast(
        partner_longest_period_months_unemployed_seeking as string
    ) partner_longest_period_months_unemployed_seeking,
    safe_cast(
        total_hours_week_personally_spend_housework as string
    ) total_hours_week_personally_spend_housework,
    safe_cast(
        total_hours_week_partner_spends_housework as string
    ) total_hours_week_partner_spends_housework,
    safe_cast(
        frequency_disagree_husband_wife_partner_money as string
    ) frequency_disagree_husband_wife_partner_money,
    safe_cast(
        interviewer_code_partner_paid_work as string
    ) interviewer_code_partner_paid_work,
    safe_cast(
        partner_work_involve_working_evenings_nights as string
    ) partner_work_involve_working_evenings_nights,
    safe_cast(
        partner_work_involve_working_overtime_short as string
    ) partner_work_involve_working_overtime_short,
    safe_cast(
        partner_work_involve_working_weekends_frequency as string
    ) partner_work_involve_working_weekends_frequency,
    safe_cast(
        interviewer_code_respondent_main_activity_retired as string
    ) interviewer_code_respondent_main_activity_retired,
    safe_cast(year_retirement as string) year_retirement,
    safe_cast(
        wanted_retire_preferred_continue_paid_work as string
    ) wanted_retire_preferred_continue_paid_work,
    safe_cast(
        interviewer_code_respondent_over_45_years as string
    ) interviewer_code_respondent_over_45_years,
    safe_cast(
        what_age_would_like_would_have_liked_retire as string
    ) what_age_would_like_would_have_liked_retire,
    safe_cast(
        plan_having_child_within_next_3_years as string
    ) plan_having_child_within_next_3_years,
    safe_cast(
        wrong_make_exaggerated_false_insurance_claim as string
    ) wrong_make_exaggerated_false_insurance_claim,
    safe_cast(
        wrong_buy_something_might_stolen as string
    ) wrong_buy_something_might_stolen,
    safe_cast(wrong_commit_traffic_offence as string) wrong_commit_traffic_offence,
    safe_cast(
        likelihood_caught_if_made_exaggerated_false as string
    ) likelihood_caught_if_made_exaggerated_false,
    safe_cast(
        likelihood_caught_if_bought_something_might as string
    ) likelihood_caught_if_bought_something_might,
    safe_cast(
        likelihood_caught_if_committed_traffic_offence as string
    ) likelihood_caught_if_committed_traffic_offence,
    safe_cast(
        police_doing_good_bad_job_country as string
    ) police_doing_good_bad_job_country,
    safe_cast(
        approached_stopped_contacted_police_2_years as string
    ) approached_stopped_contacted_police_2_years,
    safe_cast(
        satisfied_treatment_from_police_when_contacted as string
    ) satisfied_treatment_from_police_when_contacted,
    safe_cast(police_treat_victims_rich_poor as string) police_treat_victims_rich_poor,
    safe_cast(
        police_treat_victims_different_races_ethnic as string
    ) police_treat_victims_different_races_ethnic,
    safe_cast(
        successful_police_preventing_crimes_country as int64
    ) successful_police_preventing_crimes_country,
    safe_cast(
        successful_police_catching_house_burglars_country as int64
    ) successful_police_catching_house_burglars_country,
    safe_cast(
        quickly_would_police_arrive_violent_crime as int64
    ) quickly_would_police_arrive_violent_crime,
    safe_cast(
        frequency_do_police_treat_people_country_respect as string
    ) frequency_do_police_treat_people_country_respect,
    safe_cast(
        frequency_do_police_make_fair_impartial as string
    ) frequency_do_police_make_fair_impartial,
    safe_cast(
        frequency_do_police_explain_their_decisions as string
    ) frequency_do_police_explain_their_decisions,
    safe_cast(duty_back_decisions_made_police as int64) duty_back_decisions_made_police,
    safe_cast(duty_do_what_police_say as int64) duty_do_what_police_say,
    safe_cast(duty_do_what_police_say_2 as int64) duty_do_what_police_say_2,
    safe_cast(
        police_have_same_sense_right_wrong_me as string
    ) police_have_same_sense_right_wrong_me,
    safe_cast(
        police_stand_up_values_important_people_like_me as string
    ) police_stand_up_values_important_people_like_me,
    safe_cast(i_generally_support_police_act as string) i_generally_support_police_act,
    safe_cast(
        decisions_actions_police_unduly_influenced_political as string
    ) decisions_actions_police_unduly_influenced_political,
    safe_cast(
        frequency_do_police_country_take_bribes as int64
    ) frequency_do_police_country_take_bribes,
    safe_cast(
        courts_doing_good_bad_job_country as string
    ) courts_doing_good_bad_job_country,
    safe_cast(
        frequency_courts_make_mistakes_let_guilty as int64
    ) frequency_courts_make_mistakes_let_guilty,
    safe_cast(
        frequency_courts_make_fair_impartial_decisions as int64
    ) frequency_courts_make_fair_impartial_decisions,
    safe_cast(
        more_likely_found_rich_poor_falsely_accused as string
    ) more_likely_found_rich_poor_falsely_accused,
    safe_cast(
        more_likely_found_two_different_races_ethnic as string
    ) more_likely_found_two_different_races_ethnic,
    safe_cast(
        frequency_judges_country_take_bribes as int64
    ) frequency_judges_country_take_bribes,
    safe_cast(
        courts_protect_rich_powerful_over_ordinary as string
    ) courts_protect_rich_powerful_over_ordinary,
    safe_cast(
        people_who_break_law_much_harsher_sentences as string
    ) people_who_break_law_much_harsher_sentences,
    safe_cast(
        everyone_duty_back_court_final_verdict as string
    ) everyone_duty_back_court_final_verdict,
    safe_cast(
        all_laws_should_strictly_obeyed as string
    ) all_laws_should_strictly_obeyed,
    safe_cast(
        doing_right_thing_sometimes_means_breaking_law as string
    ) doing_right_thing_sometimes_means_breaking_law,
    safe_cast(
        courts_decisions_unduly_influenced_political_pressure as string
    ) courts_decisions_unduly_influenced_political_pressure,
    safe_cast(sentence_25_year_old_male as string) sentence_25_year_old_male,
    safe_cast(long_should_he_spend_prison as string) long_should_he_spend_prison,
    safe_cast(
        likelihood_call_police_if_see_man as string
    ) likelihood_call_police_if_see_man,
    safe_cast(
        willing_identify_person_who_had_done_it as string
    ) willing_identify_person_who_had_done_it,
    safe_cast(
        willing_give_evidence_court_against_accused as string
    ) willing_give_evidence_court_against_accused,
    safe_cast(
        frequency_made_exaggerated_false_insurance_claim as string
    ) frequency_made_exaggerated_false_insurance_claim,
    safe_cast(
        frequency_bought_something_might_stolen_5_years as string
    ) frequency_bought_something_might_stolen_5_years,
    safe_cast(
        frequency_committed_traffic_offence_5_years as string
    ) frequency_committed_traffic_offence_5_years,
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
    safe_cast(legal_marital_status_ireland as string) legal_marital_status_ireland,
    safe_cast(legal_marital_status_2 as string) legal_marital_status_2,
    safe_cast(children_living_home_not as string) children_living_home_not,
    safe_cast(children_living_at_home as string) children_living_at_home,
    safe_cast(
        fixed_line_telephone_accommodation as string
    ) fixed_line_telephone_accommodation,
    safe_cast(domicile_type as string) domicile_type,
    safe_cast(highest_education as string) highest_education,
    safe_cast(highest_education_es_isced as string) highest_education_es_isced,
    safe_cast(highest_education_belgium_2 as string) highest_education_belgium_2,
    safe_cast(highest_education_bulgaria_2 as string) highest_education_bulgaria_2,
    safe_cast(highest_education_switzerland as string) highest_education_switzerland,
    safe_cast(highest_education_cyprus_3 as string) highest_education_cyprus_3,
    safe_cast(highest_education_czechia as string) highest_education_czechia,
    safe_cast(highest_education_germany_9 as string) highest_education_germany_9,
    safe_cast(highest_education_germany_7 as string) highest_education_germany_7,
    safe_cast(highest_education_germany_8 as string) highest_education_germany_8,
    safe_cast(highest_education_denmark as string) highest_education_denmark,
    safe_cast(highest_education_estonia as string) highest_education_estonia,
    safe_cast(highest_education_spain_4 as string) highest_education_spain_4,
    safe_cast(highest_education_finland as string) highest_education_finland,
    safe_cast(highest_education_france as string) highest_education_france,
    safe_cast(
        highest_education_united_kingdom_7 as string
    ) highest_education_united_kingdom_7,
    safe_cast(
        highest_education_united_kingdom_6 as string
    ) highest_education_united_kingdom_6,
    safe_cast(
        age_when_completed_full_time_education as int64
    ) age_when_completed_full_time_education,
    safe_cast(highest_education_greece_2 as string) highest_education_greece_2,
    safe_cast(highest_education_croatia_2 as string) highest_education_croatia_2,
    safe_cast(highest_education_hungary_2 as string) highest_education_hungary_2,
    safe_cast(highest_education_ireland as string) highest_education_ireland,
    safe_cast(
        highest_education_israeli_education_israel_3 as string
    ) highest_education_israeli_education_israel_3,
    safe_cast(
        highest_education_russian_education_israel_2 as string
    ) highest_education_russian_education_israel_2,
    safe_cast(highest_education_lithuania as string) highest_education_lithuania,
    safe_cast(
        highest_education_netherlands_2 as string
    ) highest_education_netherlands_2,
    safe_cast(highest_education_norway_2 as string) highest_education_norway_2,
    safe_cast(highest_education_poland_4 as string) highest_education_poland_4,
    safe_cast(year_school_completion_poland as string) year_school_completion_poland,
    safe_cast(
        tertiary_education_lower_higher_single_tier as string
    ) tertiary_education_lower_higher_single_tier,
    safe_cast(highest_education_portugal_2 as string) highest_education_portugal_2,
    safe_cast(
        highest_education_russian_federation as string
    ) highest_education_russian_federation,
    safe_cast(highest_education_sweden as string) highest_education_sweden,
    safe_cast(highest_education_slovenia_2 as string) highest_education_slovenia_2,
    safe_cast(highest_education_slovakia as string) highest_education_slovakia,
    safe_cast(highest_education_ukraine as string) highest_education_ukraine,
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
    safe_cast(allowed_choose_change_pace_work as int64) allowed_choose_change_pace_work,
    safe_cast(contracted_weekly_hours as int64) contracted_weekly_hours,
    safe_cast(total_weekly_hours as int64) total_weekly_hours,
    safe_cast(industry_nace_rev_2 as string) industry_nace_rev_2,
    safe_cast(
        what_type_organisation_work_worked as string
    ) what_type_organisation_work_worked,
    safe_cast(occupation_isco88_com as string) occupation_isco88_com,
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
        borrow_money_make_ends_meet_difficult_easy as string
    ) borrow_money_make_ends_meet_difficult_easy,
    safe_cast(
        interviewer_code_lives_husband_wife_partner_3 as string
    ) interviewer_code_lives_husband_wife_partner_3,
    safe_cast(partner_highest_education as string) partner_highest_education,
    safe_cast(
        partner_highest_education_es_isced as string
    ) partner_highest_education_es_isced,
    safe_cast(
        partner_highest_education_belgium_2 as string
    ) partner_highest_education_belgium_2,
    safe_cast(
        partner_highest_education_bulgaria_2 as string
    ) partner_highest_education_bulgaria_2,
    safe_cast(
        partner_highest_education_switzerland as string
    ) partner_highest_education_switzerland,
    safe_cast(
        partner_highest_education_cyprus_3 as string
    ) partner_highest_education_cyprus_3,
    safe_cast(
        partner_highest_education_czechia as string
    ) partner_highest_education_czechia,
    safe_cast(
        partner_highest_education_germany_9 as string
    ) partner_highest_education_germany_9,
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
        partner_highest_education_spain_4 as string
    ) partner_highest_education_spain_4,
    safe_cast(
        partner_highest_education_finland as string
    ) partner_highest_education_finland,
    safe_cast(
        partner_highest_education_france as string
    ) partner_highest_education_france,
    safe_cast(
        partner_highest_education_united_kingdom_7 as string
    ) partner_highest_education_united_kingdom_7,
    safe_cast(
        partner_highest_education_united_kingdom_6 as string
    ) partner_highest_education_united_kingdom_6,
    safe_cast(
        partner_age_when_completed_full_time as int64
    ) partner_age_when_completed_full_time,
    safe_cast(
        partner_highest_education_greece_2 as string
    ) partner_highest_education_greece_2,
    safe_cast(
        partner_highest_education_croatia_2 as string
    ) partner_highest_education_croatia_2,
    safe_cast(
        partner_highest_education_hungary_2 as string
    ) partner_highest_education_hungary_2,
    safe_cast(
        partner_highest_education_ireland as string
    ) partner_highest_education_ireland,
    safe_cast(
        partner_highest_education_israeli_education_israel_3 as string
    ) partner_highest_education_israeli_education_israel_3,
    safe_cast(
        partner_highest_education_russian_education_israel_2 as string
    ) partner_highest_education_russian_education_israel_2,
    safe_cast(
        partner_highest_education_lithuania as string
    ) partner_highest_education_lithuania,
    safe_cast(
        partner_highest_education_netherlands_2 as string
    ) partner_highest_education_netherlands_2,
    safe_cast(
        partner_highest_education_norway_2 as string
    ) partner_highest_education_norway_2,
    safe_cast(
        partner_highest_education_poland_4 as string
    ) partner_highest_education_poland_4,
    safe_cast(
        partner_year_school_completion_poland as string
    ) partner_year_school_completion_poland,
    safe_cast(
        partner_tertiary_education_lower_higher_single as string
    ) partner_tertiary_education_lower_higher_single,
    safe_cast(
        partner_highest_education_portugal_2 as string
    ) partner_highest_education_portugal_2,
    safe_cast(
        partner_highest_education_russian_federation as string
    ) partner_highest_education_russian_federation,
    safe_cast(
        partner_highest_education_sweden as string
    ) partner_highest_education_sweden,
    safe_cast(
        partner_highest_education_slovenia_2 as string
    ) partner_highest_education_slovenia_2,
    safe_cast(
        partner_highest_education_slovakia as string
    ) partner_highest_education_slovakia,
    safe_cast(
        partner_highest_education_ukraine as string
    ) partner_highest_education_ukraine,
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
    safe_cast(partner_main_activity_7_days as string) partner_main_activity_7_days,
    safe_cast(
        partner_control_paid_work_7_days as string
    ) partner_control_paid_work_7_days,
    safe_cast(occupation_partner_isco88_com as string) occupation_partner_isco88_com,
    safe_cast(partner_employment_relation as string) partner_employment_relation,
    safe_cast(number_employees_partner_has as string) number_employees_partner_has,
    safe_cast(
        partner_responsible_supervising_other_employees as string
    ) partner_responsible_supervising_other_employees,
    safe_cast(
        number_people_partner_responsible_job as string
    ) number_people_partner_responsible_job,
    safe_cast(
        hours_normally_worked_week_main_job as int64
    ) hours_normally_worked_week_main_job,
    safe_cast(father_highest_education as string) father_highest_education,
    safe_cast(
        father_highest_education_es_isced as string
    ) father_highest_education_es_isced,
    safe_cast(
        father_highest_education_belgium_2 as string
    ) father_highest_education_belgium_2,
    safe_cast(
        father_highest_education_bulgaria_2 as string
    ) father_highest_education_bulgaria_2,
    safe_cast(
        father_highest_education_switzerland as string
    ) father_highest_education_switzerland,
    safe_cast(
        father_highest_education_cyprus_3 as string
    ) father_highest_education_cyprus_3,
    safe_cast(
        father_highest_education_czechia as string
    ) father_highest_education_czechia,
    safe_cast(
        father_highest_education_germany_9 as string
    ) father_highest_education_germany_9,
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
        father_highest_education_spain_4 as string
    ) father_highest_education_spain_4,
    safe_cast(
        father_highest_education_finland as string
    ) father_highest_education_finland,
    safe_cast(
        father_highest_education_france as string
    ) father_highest_education_france,
    safe_cast(
        father_highest_education_united_kingdom_7 as string
    ) father_highest_education_united_kingdom_7,
    safe_cast(
        father_highest_education_united_kingdom_6 as string
    ) father_highest_education_united_kingdom_6,
    safe_cast(
        father_age_when_completed_full_time as int64
    ) father_age_when_completed_full_time,
    safe_cast(
        father_highest_education_greece_2 as string
    ) father_highest_education_greece_2,
    safe_cast(
        father_highest_education_croatia_2 as string
    ) father_highest_education_croatia_2,
    safe_cast(
        father_highest_education_hungary_2 as string
    ) father_highest_education_hungary_2,
    safe_cast(
        father_highest_education_ireland as string
    ) father_highest_education_ireland,
    safe_cast(
        father_highest_education_israeli_education_israel_3 as string
    ) father_highest_education_israeli_education_israel_3,
    safe_cast(
        father_highest_education_russian_education_israel_2 as string
    ) father_highest_education_russian_education_israel_2,
    safe_cast(
        father_highest_education_lithuania as string
    ) father_highest_education_lithuania,
    safe_cast(
        father_highest_education_netherlands_2 as string
    ) father_highest_education_netherlands_2,
    safe_cast(
        father_highest_education_norway_2 as string
    ) father_highest_education_norway_2,
    safe_cast(
        father_highest_education_poland_4 as string
    ) father_highest_education_poland_4,
    safe_cast(
        father_highest_education_portugal_2 as string
    ) father_highest_education_portugal_2,
    safe_cast(
        father_highest_education_russian_federation as string
    ) father_highest_education_russian_federation,
    safe_cast(
        father_highest_education_sweden as string
    ) father_highest_education_sweden,
    safe_cast(
        father_highest_education_slovenia_2 as string
    ) father_highest_education_slovenia_2,
    safe_cast(
        father_highest_education_slovakia as string
    ) father_highest_education_slovakia,
    safe_cast(
        father_highest_education_ukraine as string
    ) father_highest_education_ukraine,
    safe_cast(
        father_employment_status_when_respondent_14 as string
    ) father_employment_status_when_respondent_14,
    safe_cast(number_employees_father_had as string) number_employees_father_had,
    safe_cast(
        father_responsible_supervising_other_employees as string
    ) father_responsible_supervising_other_employees,
    safe_cast(
        father_occupation_when_respondent_14 as string
    ) father_occupation_when_respondent_14,
    safe_cast(mother_highest_education as string) mother_highest_education,
    safe_cast(
        mother_highest_education_es_isced as string
    ) mother_highest_education_es_isced,
    safe_cast(
        mother_highest_education_belgium_2 as string
    ) mother_highest_education_belgium_2,
    safe_cast(
        mother_highest_education_bulgaria_2 as string
    ) mother_highest_education_bulgaria_2,
    safe_cast(
        mother_highest_education_switzerland as string
    ) mother_highest_education_switzerland,
    safe_cast(
        mother_highest_education_cyprus_3 as string
    ) mother_highest_education_cyprus_3,
    safe_cast(
        mother_highest_education_czechia as string
    ) mother_highest_education_czechia,
    safe_cast(
        mother_highest_education_germany_9 as string
    ) mother_highest_education_germany_9,
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
        mother_highest_education_spain_4 as string
    ) mother_highest_education_spain_4,
    safe_cast(
        mother_highest_education_finland as string
    ) mother_highest_education_finland,
    safe_cast(
        mother_highest_education_france as string
    ) mother_highest_education_france,
    safe_cast(
        mother_highest_education_united_kingdom_7 as string
    ) mother_highest_education_united_kingdom_7,
    safe_cast(
        mother_highest_education_united_kingdom_6 as string
    ) mother_highest_education_united_kingdom_6,
    safe_cast(
        mother_age_when_completed_full_time as int64
    ) mother_age_when_completed_full_time,
    safe_cast(
        mother_highest_education_greece_2 as string
    ) mother_highest_education_greece_2,
    safe_cast(
        mother_highest_education_croatia_2 as string
    ) mother_highest_education_croatia_2,
    safe_cast(
        mother_highest_education_hungary_2 as string
    ) mother_highest_education_hungary_2,
    safe_cast(
        mother_highest_education_ireland as string
    ) mother_highest_education_ireland,
    safe_cast(
        mother_highest_education_israeli_education_israel_3 as string
    ) mother_highest_education_israeli_education_israel_3,
    safe_cast(
        mother_highest_education_russian_education_israel_2 as string
    ) mother_highest_education_russian_education_israel_2,
    safe_cast(
        mother_highest_education_lithuania as string
    ) mother_highest_education_lithuania,
    safe_cast(
        mother_highest_education_netherlands_2 as string
    ) mother_highest_education_netherlands_2,
    safe_cast(
        mother_highest_education_norway_2 as string
    ) mother_highest_education_norway_2,
    safe_cast(
        mother_highest_education_poland_4 as string
    ) mother_highest_education_poland_4,
    safe_cast(
        mother_highest_education_portugal_2 as string
    ) mother_highest_education_portugal_2,
    safe_cast(
        mother_highest_education_russian_federation as string
    ) mother_highest_education_russian_federation,
    safe_cast(
        mother_highest_education_sweden as string
    ) mother_highest_education_sweden,
    safe_cast(
        mother_highest_education_slovenia_2 as string
    ) mother_highest_education_slovenia_2,
    safe_cast(
        mother_highest_education_slovakia as string
    ) mother_highest_education_slovakia,
    safe_cast(
        mother_highest_education_ukraine as string
    ) mother_highest_education_ukraine,
    safe_cast(
        mother_employment_status_when_respondent_14 as string
    ) mother_employment_status_when_respondent_14,
    safe_cast(number_employees_mother_had as string) number_employees_mother_had,
    safe_cast(
        mother_responsible_supervising_other_employees as string
    ) mother_responsible_supervising_other_employees,
    safe_cast(
        mother_occupation_when_respondent_14 as string
    ) mother_occupation_when_respondent_14,
    safe_cast(
        improve_skills_course_lecture_conference_12 as string
    ) improve_skills_course_lecture_conference_12,
    safe_cast(region_code as string) region_code,
    safe_cast(regional_unit as string) regional_unit,
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
    safe_cast(
        place_interview_east_west_germany as string
    ) place_interview_east_west_germany,
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
        administration_split_ballot_mtmm_3 as string
    ) administration_split_ballot_mtmm_3,
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
from {{ set_datalake_project("gb_eric_ess_staging.round_05") }} as t
