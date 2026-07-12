{{
    config(
        schema="gb_eric_ess",
        alias="round_03",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2006, "end": 2011, "interval": 1},
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
    safe_cast(
        politics_too_complicated_understand as string
    ) politics_too_complicated_understand,
    safe_cast(
        making_mind_up_political_issues as string
    ) making_mind_up_political_issues,
    safe_cast(trust_parliament as int64) trust_parliament,
    safe_cast(trust_legal_system as int64) trust_legal_system,
    safe_cast(trust_police as int64) trust_police,
    safe_cast(trust_politicians as int64) trust_politicians,
    safe_cast(trust_political_parties as int64) trust_political_parties,
    safe_cast(trust_european_parliament as int64) trust_european_parliament,
    safe_cast(trust_united_nations as int64) trust_united_nations,
    safe_cast(voted_last_national_election as string) voted_last_national_election,
    safe_cast(party_voted_austria_4 as string) party_voted_austria_4,
    safe_cast(party_voted_belgium_5 as string) party_voted_belgium_5,
    safe_cast(party_voted_bulgaria_6 as string) party_voted_bulgaria_6,
    safe_cast(party_voted_switzerland_8 as string) party_voted_switzerland_8,
    safe_cast(party_voted_cyprus_4 as string) party_voted_cyprus_4,
    safe_cast(party_voted_1_germany_5 as string) party_voted_1_germany_5,
    safe_cast(party_voted_2_germany_5 as string) party_voted_2_germany_5,
    safe_cast(party_voted_denmark_4 as string) party_voted_denmark_4,
    safe_cast(party_voted_estonia_8 as string) party_voted_estonia_8,
    safe_cast(party_voted_spain_6 as string) party_voted_spain_6,
    safe_cast(party_voted_finland_7 as string) party_voted_finland_7,
    safe_cast(party_voted_france_ballot_1_6 as string) party_voted_france_ballot_1_6,
    safe_cast(party_voted_united_kingdom_5 as string) party_voted_united_kingdom_5,
    safe_cast(party_voted_hungary_8 as string) party_voted_hungary_8,
    safe_cast(party_voted_ireland_3 as string) party_voted_ireland_3,
    safe_cast(party_voted_latvia_3 as string) party_voted_latvia_3,
    safe_cast(party_voted_netherlands_8 as string) party_voted_netherlands_8,
    safe_cast(party_voted_norway_4 as string) party_voted_norway_4,
    safe_cast(party_voted_poland_5 as string) party_voted_poland_5,
    safe_cast(party_voted_portugal_5 as string) party_voted_portugal_5,
    safe_cast(party_voted_romania_2 as string) party_voted_romania_2,
    safe_cast(
        party_voted_russian_federation_5 as string
    ) party_voted_russian_federation_5,
    safe_cast(party_voted_sweden_5 as string) party_voted_sweden_5,
    safe_cast(party_voted_slovenia_6 as string) party_voted_slovenia_6,
    safe_cast(party_voted_slovakia_5 as string) party_voted_slovakia_5,
    safe_cast(party_voted_ukraine_2 as string) party_voted_ukraine_2,
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
    safe_cast(party_feel_close_austria_4 as string) party_feel_close_austria_4,
    safe_cast(party_feel_close_belgium_5 as string) party_feel_close_belgium_5,
    safe_cast(party_feel_close_bulgaria_7 as string) party_feel_close_bulgaria_7,
    safe_cast(party_feel_close_switzerland_8 as string) party_feel_close_switzerland_8,
    safe_cast(party_feel_close_cyprus_4 as string) party_feel_close_cyprus_4,
    safe_cast(party_feel_close_germany_5 as string) party_feel_close_germany_5,
    safe_cast(party_feel_close_denmark_4 as string) party_feel_close_denmark_4,
    safe_cast(party_feel_close_estonia_8 as string) party_feel_close_estonia_8,
    safe_cast(party_feel_close_spain_7 as string) party_feel_close_spain_7,
    safe_cast(party_feel_close_finland_8 as string) party_feel_close_finland_8,
    safe_cast(party_feel_close_france_7 as string) party_feel_close_france_7,
    safe_cast(
        party_feel_close_united_kingdom_5 as string
    ) party_feel_close_united_kingdom_5,
    safe_cast(party_feel_close_hungary_9 as string) party_feel_close_hungary_9,
    safe_cast(party_feel_close_ireland_6 as string) party_feel_close_ireland_6,
    safe_cast(party_feel_close_latvia_3 as string) party_feel_close_latvia_3,
    safe_cast(party_feel_close_netherlands_8 as string) party_feel_close_netherlands_8,
    safe_cast(party_feel_close_norway_4 as string) party_feel_close_norway_4,
    safe_cast(party_feel_close_poland_8 as string) party_feel_close_poland_8,
    safe_cast(party_feel_close_portugal_6 as string) party_feel_close_portugal_6,
    safe_cast(party_feel_close_romania_2 as string) party_feel_close_romania_2,
    safe_cast(
        party_feel_close_russian_federation_5 as string
    ) party_feel_close_russian_federation_5,
    safe_cast(party_feel_close_sweden_5 as string) party_feel_close_sweden_5,
    safe_cast(party_feel_close_slovenia_6 as string) party_feel_close_slovenia_6,
    safe_cast(party_feel_close_slovakia_5 as string) party_feel_close_slovakia_5,
    safe_cast(party_feel_close_ukraine_4 as string) party_feel_close_ukraine_4,
    safe_cast(how_close_to_party as string) how_close_to_party,
    safe_cast(member_political_party as string) member_political_party,
    safe_cast(member_party_austria as string) member_party_austria,
    safe_cast(member_party_belgium_3 as string) member_party_belgium_3,
    safe_cast(member_party_bulgaria_3 as string) member_party_bulgaria_3,
    safe_cast(member_party_switzerland_3 as string) member_party_switzerland_3,
    safe_cast(member_party_cyprus as string) member_party_cyprus,
    safe_cast(member_party_germany_2 as string) member_party_germany_2,
    safe_cast(member_party_denmark_2 as string) member_party_denmark_2,
    safe_cast(member_party_estonia_3 as string) member_party_estonia_3,
    safe_cast(member_party_spain_2 as string) member_party_spain_2,
    safe_cast(member_party_finland_3 as string) member_party_finland_3,
    safe_cast(member_party_france_3 as string) member_party_france_3,
    safe_cast(member_party_united_kingdom_2 as string) member_party_united_kingdom_2,
    safe_cast(member_party_hungary_3 as string) member_party_hungary_3,
    safe_cast(member_party_ireland_3 as string) member_party_ireland_3,
    safe_cast(member_party_latvia as string) member_party_latvia,
    safe_cast(member_party_netherlands_3 as string) member_party_netherlands_3,
    safe_cast(member_party_norway_2 as string) member_party_norway_2,
    safe_cast(member_party_poland_3 as string) member_party_poland_3,
    safe_cast(member_party_portugal_2 as string) member_party_portugal_2,
    safe_cast(member_party_romania_2 as string) member_party_romania_2,
    safe_cast(
        member_party_russian_federation_3 as string
    ) member_party_russian_federation_3,
    safe_cast(member_party_sweden_2 as string) member_party_sweden_2,
    safe_cast(member_party_slovenia_2 as string) member_party_slovenia_2,
    safe_cast(member_party_slovakia_2 as string) member_party_slovakia_2,
    safe_cast(member_party_ukraine_3 as string) member_party_ukraine_3,
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
    safe_cast(
        likelihood_terrorist_attack_europe_during_next as string
    ) likelihood_terrorist_attack_europe_during_next,
    safe_cast(
        likelihood_terrorist_attack_country_during_next as string
    ) likelihood_terrorist_attack_country_during_next,
    safe_cast(
        terrorist_suspect_prison_until_police_satisfied as string
    ) terrorist_suspect_prison_until_police_satisfied,
    safe_cast(
        torture_country_never_justified_even_prevent as string
    ) torture_country_never_justified_even_prevent,
    safe_cast(subjective_general_health as string) subjective_general_health,
    safe_cast(hampered_by_illness as string) hampered_by_illness,
    safe_cast(belongs_to_religion as string) belongs_to_religion,
    safe_cast(
        religion_denomination_belonging_present as string
    ) religion_denomination_belonging_present,
    safe_cast(
        ever_belonging_particular_religion_denomination as string
    ) ever_belonging_particular_religion_denomination,
    safe_cast(
        religion_denomination_belonging_past as string
    ) religion_denomination_belonging_past,
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
    safe_cast(citizenship_4 as string) citizenship_4,
    safe_cast(born_in_country as string) born_in_country,
    safe_cast(country_birth_4 as string) country_birth_4,
    safe_cast(
        long_ago_first_came_live_country as string
    ) long_ago_first_came_live_country,
    safe_cast(language_home_first_mentioned as string) language_home_first_mentioned,
    safe_cast(language_home_second_mentioned as string) language_home_second_mentioned,
    safe_cast(belong_ethnic_minority as string) belong_ethnic_minority,
    safe_cast(father_born_in_country as string) father_born_in_country,
    safe_cast(country_birth_father_4 as string) country_birth_father_4,
    safe_cast(mother_born_in_country as string) mother_born_in_country,
    safe_cast(country_birth_mother_4 as string) country_birth_mother_4,
    safe_cast(
        paid_employment_apprenticeship_least_3_months as string
    ) paid_employment_apprenticeship_least_3_months,
    safe_cast(
        year_first_started_paid_employment_apprenticeship as string
    ) year_first_started_paid_employment_apprenticeship,
    safe_cast(
        year_first_left_parents_living_separately as string
    ) year_first_left_parents_living_separately,
    safe_cast(
        ever_lived_spouse_partner_3_months_more as string
    ) ever_lived_spouse_partner_3_months_more,
    safe_cast(
        year_first_lived_spouse_partner_3_months_more as string
    ) year_first_lived_spouse_partner_3_months_more,
    safe_cast(ever_married as string) ever_married,
    safe_cast(year_first_married as string) year_first_married,
    safe_cast(
        ever_given_birth_fathered_child as string
    ) ever_given_birth_fathered_child,
    safe_cast(
        number_children_ever_given_birth_fathered as string
    ) number_children_ever_given_birth_fathered,
    safe_cast(year_first_child_born as string) year_first_child_born,
    safe_cast(year_youngest_child_born as string) year_youngest_child_born,
    safe_cast(number_grandchildren as string) number_grandchildren,
    safe_cast(year_first_grandchild_born as string) year_first_grandchild_born,
    safe_cast(have_any_great_grandchildren as string) have_any_great_grandchildren,
    safe_cast(month_born as string) month_born,
    safe_cast(
        interviewer_code_split_ballot_ask_male_female as string
    ) interviewer_code_split_ballot_ask_male_female,
    safe_cast(age_become_adults_split_ballot as string) age_become_adults_split_ballot,
    safe_cast(
        age_reach_middle_age_split_ballot as string
    ) age_reach_middle_age_split_ballot,
    safe_cast(age_reach_old_age_split_ballot as string) age_reach_old_age_split_ballot,
    safe_cast(
        considered_adult_important_have_left_parental as string
    ) considered_adult_important_have_left_parental,
    safe_cast(
        considered_adult_important_have_full_time as string
    ) considered_adult_important_have_full_time,
    safe_cast(
        considered_adult_important_have_lived_spouse as string
    ) considered_adult_important_have_lived_spouse,
    safe_cast(
        considered_adult_important_have_become_mother as string
    ) considered_adult_important_have_become_mother,
    safe_cast(
        considered_old_important_physically_frail_split as string
    ) considered_old_important_physically_frail_split,
    safe_cast(
        considered_old_important_grandmother_grandfather_split as string
    ) considered_old_important_grandmother_grandfather_split,
    safe_cast(
        considered_old_important_need_others_look as string
    ) considered_old_important_need_others_look,
    safe_cast(
        start_living_partner_not_married_ideal as string
    ) start_living_partner_not_married_ideal,
    safe_cast(
        get_married_live_husband_wife_ideal as string
    ) get_married_live_husband_wife_ideal,
    safe_cast(
        become_mother_father_ideal_age_split_ballot as string
    ) become_mother_father_ideal_age_split_ballot,
    safe_cast(
        retire_permanently_ideal_age_split_ballot as string
    ) retire_permanently_ideal_age_split_ballot,
    safe_cast(
        leave_full_time_education_age_too as string
    ) leave_full_time_education_age_too,
    safe_cast(
        have_sexual_intercourse_age_too_young as string
    ) have_sexual_intercourse_age_too_young,
    safe_cast(
        start_living_partner_not_married_age as string
    ) start_living_partner_not_married_age,
    safe_cast(
        get_married_live_husband_wife_age as string
    ) get_married_live_husband_wife_age,
    safe_cast(
        become_mother_father_age_too_young_split_ballot as string
    ) become_mother_father_age_too_young_split_ballot,
    safe_cast(
        retire_permanently_age_too_young_split_ballot as string
    ) retire_permanently_age_too_young_split_ballot,
    safe_cast(
        still_living_parents_age_too_old_split_ballot as string
    ) still_living_parents_age_too_old_split_ballot,
    safe_cast(
        consider_having_more_children_age_too as string
    ) consider_having_more_children_age_too,
    safe_cast(working_20_hours_more_per_week as string) working_20_hours_more_per_week,
    safe_cast(
        approve_if_person_chooses_never_have as string
    ) approve_if_person_chooses_never_have,
    safe_cast(
        approve_if_person_lives_partner_not as string
    ) approve_if_person_lives_partner_not,
    safe_cast(
        approve_if_person_have_child_partner as string
    ) approve_if_person_have_child_partner,
    safe_cast(
        approve_if_person_has_full_time as string
    ) approve_if_person_has_full_time,
    safe_cast(
        approve_if_person_gets_divorced_while as string
    ) approve_if_person_gets_divorced_while,
    safe_cast(
        most_people_react_if_person_became as string
    ) most_people_react_if_person_became,
    safe_cast(
        most_people_react_if_person_carried as string
    ) most_people_react_if_person_carried,
    safe_cast(
        most_people_react_if_person_chose as string
    ) most_people_react_if_person_chose,
    safe_cast(
        most_people_react_if_person_lived as string
    ) most_people_react_if_person_lived,
    safe_cast(
        most_people_react_if_person_had as string
    ) most_people_react_if_person_had,
    safe_cast(
        most_people_react_if_person_had_2 as string
    ) most_people_react_if_person_had_2,
    safe_cast(
        most_people_react_if_person_got as string
    ) most_people_react_if_person_got,
    safe_cast(
        plan_future_take_each_day_it_comes as int64
    ) plan_future_take_each_day_it_comes,
    safe_cast(worried_income_old_age_will_not as int64) worried_income_old_age_will_not,
    safe_cast(
        mainly_responsible_providing_people_adequate_living as int64
    ) mainly_responsible_providing_people_adequate_living,
    safe_cast(
        saving_saved_live_comfortably_old_age as string
    ) saving_saved_live_comfortably_old_age,
    safe_cast(
        involved_work_voluntary_charitable_organisations_frequency as string
    ) involved_work_voluntary_charitable_organisations_frequency,
    safe_cast(
        help_others_not_counting_family_work as string
    ) help_others_not_counting_family_work,
    safe_cast(
        help_attend_activities_organised_local_area as string
    ) help_attend_activities_organised_local_area,
    safe_cast(always_optimistic_my_future as string) always_optimistic_my_future,
    safe_cast(
        general_feel_very_positive_myself as string
    ) general_feel_very_positive_myself,
    safe_cast(times_feel_if_i_am_failure as string) times_feel_if_i_am_failure,
    safe_cast(life_close_i_would_like_it as string) life_close_i_would_like_it,
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
        had_lot_energy_frequency_past_week as string
    ) had_lot_energy_frequency_past_week,
    safe_cast(
        felt_anxious_frequency_past_week as string
    ) felt_anxious_frequency_past_week,
    safe_cast(felt_tired_frequency_past_week as string) felt_tired_frequency_past_week,
    safe_cast(
        absorbed_doing_frequency_past_week as string
    ) absorbed_doing_frequency_past_week,
    safe_cast(
        felt_calm_peaceful_frequency_past_week as string
    ) felt_calm_peaceful_frequency_past_week,
    safe_cast(felt_bored_frequency_past_week as string) felt_bored_frequency_past_week,
    safe_cast(
        felt_rested_when_woke_up_morning as string
    ) felt_rested_when_woke_up_morning,
    safe_cast(free_decide_live_my_life as string) free_decide_live_my_life,
    safe_cast(
        seldom_time_do_things_i_really_enjoy as string
    ) seldom_time_do_things_i_really_enjoy,
    safe_cast(
        little_chance_show_capable_i_am as string
    ) little_chance_show_capable_i_am,
    safe_cast(love_learning_new_things as string) love_learning_new_things,
    safe_cast(
        feel_accomplishment_from_what_i_do as string
    ) feel_accomplishment_from_what_i_do,
    safe_cast(like_planning_preparing_future as string) like_planning_preparing_future,
    safe_cast(when_things_go_wrong_my_life as string) when_things_go_wrong_my_life,
    safe_cast(
        my_life_involves_lot_physical_activity as string
    ) my_life_involves_lot_physical_activity,
    safe_cast(
        satisfied_life_turned_out_so_far as int64
    ) satisfied_life_turned_out_so_far,
    safe_cast(satisfied_standard_living as int64) satisfied_standard_living,
    safe_cast(
        much_time_spent_immediate_family_enjoyable as string
    ) much_time_spent_immediate_family_enjoyable,
    safe_cast(
        much_time_spent_immediate_family_stressful as string
    ) much_time_spent_immediate_family_stressful,
    safe_cast(chance_learn_new_things as string) chance_learn_new_things,
    safe_cast(
        feel_people_local_area_help_one_another as string
    ) feel_people_local_area_help_one_another,
    safe_cast(feel_people_treat_respect as string) feel_people_treat_respect,
    safe_cast(feel_people_treat_unfairly as string) feel_people_treat_unfairly,
    safe_cast(
        feel_get_recognition_deserve_what_do as string
    ) feel_get_recognition_deserve_what_do,
    safe_cast(
        feel_what_i_do_life_valuable_worthwhile as string
    ) feel_what_i_do_life_valuable_worthwhile,
    safe_cast(
        if_i_help_someone_i_expect_some_help_return as string
    ) if_i_help_someone_i_expect_some_help_return,
    safe_cast(hard_hopeful_future_world as string) hard_hopeful_future_world,
    safe_cast(
        there_people_my_life_who_care_me as string
    ) there_people_my_life_who_care_me,
    safe_cast(
        most_people_country_life_getting_worse as string
    ) most_people_country_life_getting_worse,
    safe_cast(feel_close_people_local_area as string) feel_close_people_local_area,
    safe_cast(
        ever_feel_frustrated_having_watched_too as string
    ) ever_feel_frustrated_having_watched_too,
    safe_cast(currently_paid_work_any_kind as string) currently_paid_work_any_kind,
    safe_cast(satisfied_job as int64) satisfied_job,
    safe_cast(
        satisfied_balance_between_time_job_time as int64
    ) satisfied_balance_between_time_job_time,
    safe_cast(find_job_interesting_much_time as string) find_job_interesting_much_time,
    safe_cast(find_job_stressful_much_time as string) find_job_stressful_much_time,
    safe_cast(
        become_unemployed_next_12_months_likelihood as string
    ) become_unemployed_next_12_months_likelihood,
    safe_cast(
        get_paid_appropriately_considering_efforts_achievements as string
    ) get_paid_appropriately_considering_efforts_achievements,
    safe_cast(
        important_compare_income_other_people_income as string
    ) important_compare_income_other_people_income,
    safe_cast(
        whose_income_most_likely_compare as string
    ) whose_income_most_likely_compare,
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
    safe_cast(household_member_15_gender as string) household_member_15_gender,
    safe_cast(year_of_birth as int64) year_of_birth,
    safe_cast(age_respondent_calculated as string) age_respondent_calculated,
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
        household_member_15_year_of_birth as int64
    ) household_member_15_year_of_birth,
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
        household_member_15_relationship as string
    ) household_member_15_relationship,
    safe_cast(domicile_type as string) domicile_type,
    safe_cast(highest_education_3 as string) highest_education_3,
    safe_cast(highest_education_es_isced as string) highest_education_es_isced,
    safe_cast(highest_education_belgium_4 as string) highest_education_belgium_4,
    safe_cast(highest_education_bulgaria_3 as string) highest_education_bulgaria_3,
    safe_cast(
        highest_education_switzerland_3 as string
    ) highest_education_switzerland_3,
    safe_cast(highest_education_cyprus_5 as string) highest_education_cyprus_5,
    safe_cast(highest_education_germany_10 as string) highest_education_germany_10,
    safe_cast(highest_education_denmark_2 as string) highest_education_denmark_2,
    safe_cast(highest_education_estonia_3 as string) highest_education_estonia_3,
    safe_cast(highest_education_spain_5 as string) highest_education_spain_5,
    safe_cast(highest_education_france_3 as string) highest_education_france_3,
    safe_cast(
        highest_education_united_kingdom_8 as string
    ) highest_education_united_kingdom_8,
    safe_cast(highest_education_hungary_4 as string) highest_education_hungary_4,
    safe_cast(highest_education_ireland_3 as string) highest_education_ireland_3,
    safe_cast(highest_education_latvia_3 as string) highest_education_latvia_3,
    safe_cast(
        highest_education_netherlands_3 as string
    ) highest_education_netherlands_3,
    safe_cast(highest_education_norway_4 as string) highest_education_norway_4,
    safe_cast(highest_education_poland_6 as string) highest_education_poland_6,
    safe_cast(highest_education_portugal_4 as string) highest_education_portugal_4,
    safe_cast(highest_education_romania as string) highest_education_romania,
    safe_cast(
        highest_education_russian_federation_2 as string
    ) highest_education_russian_federation_2,
    safe_cast(highest_education_sweden_2 as string) highest_education_sweden_2,
    safe_cast(highest_education_slovenia_4 as string) highest_education_slovenia_4,
    safe_cast(highest_education_slovakia_3 as string) highest_education_slovakia_3,
    safe_cast(highest_education_ukraine_3 as string) highest_education_ukraine_3,
    safe_cast(
        field_subject_highest_qualification as string
    ) field_subject_highest_qualification,
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
    safe_cast(activity_dont_know as string) activity_dont_know,
    safe_cast(activity_refusal as string) activity_refusal,
    safe_cast(activity_no_answer as string) activity_no_answer,
    safe_cast(main_activity_last_7_days as string) main_activity_last_7_days,
    safe_cast(
        main_activity_7_days_all_respondent_post_coded as string
    ) main_activity_7_days_all_respondent_post_coded,
    safe_cast(
        year_become_retired_permanently_sick_disabled as string
    ) year_become_retired_permanently_sick_disabled,
    safe_cast(
        worried_not_able_retire_age_would_like as int64
    ) worried_not_able_retire_age_would_like,
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
    safe_cast(occupation_isco88_com as string) occupation_isco88_com,
    safe_cast(industry_nace_rev_1_1 as string) industry_nace_rev_1_1,
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
    safe_cast(
        household_total_net_income_all_sources as string
    ) household_total_net_income_all_sources,
    safe_cast(feeling_about_household_income as string) feeling_about_household_income,
    safe_cast(
        borrow_money_make_ends_meet_difficult_easy as string
    ) borrow_money_make_ends_meet_difficult_easy,
    safe_cast(
        lives_husband_wife_partner_household_grid as string
    ) lives_husband_wife_partner_household_grid,
    safe_cast(partner_highest_education_3 as string) partner_highest_education_3,
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
        partner_allowed_decide_daily_work_organised as int64
    ) partner_allowed_decide_daily_work_organised,
    safe_cast(
        partner_allowed_influence_policy_decisions_activities as int64
    ) partner_allowed_influence_policy_decisions_activities,
    safe_cast(
        hours_normally_worked_week_main_job as int64
    ) hours_normally_worked_week_main_job,
    safe_cast(father_highest_education_3 as string) father_highest_education_3,
    safe_cast(
        father_employment_status_when_respondent_14 as string
    ) father_employment_status_when_respondent_14,
    safe_cast(number_employees_father_had as string) number_employees_father_had,
    safe_cast(
        father_responsible_supervising_other_employees as string
    ) father_responsible_supervising_other_employees,
    safe_cast(
        father_occupation_when_respondent_14_2 as string
    ) father_occupation_when_respondent_14_2,
    safe_cast(mother_highest_education_3 as string) mother_highest_education_3,
    safe_cast(
        mother_employment_status_when_respondent_14 as string
    ) mother_employment_status_when_respondent_14,
    safe_cast(number_employees_mother_had as string) number_employees_mother_had,
    safe_cast(
        mother_responsible_supervising_other_employees as string
    ) mother_responsible_supervising_other_employees,
    safe_cast(
        mother_occupation_when_respondent_14_2 as string
    ) mother_occupation_when_respondent_14_2,
    safe_cast(
        improve_skills_course_lecture_conference_12 as string
    ) improve_skills_course_lecture_conference_12,
    safe_cast(legal_marital_status_3 as string) legal_marital_status_3,
    safe_cast(
        currently_living_husband_wife_civil_partner as string
    ) currently_living_husband_wife_civil_partner,
    safe_cast(currently_living_partner as string) currently_living_partner,
    safe_cast(
        ever_lived_partner_without_married_2 as string
    ) ever_lived_partner_without_married_2,
    safe_cast(ever_divorced as string) ever_divorced,
    safe_cast(children_living_home_not as string) children_living_home_not,
    safe_cast(children_living_at_home as string) children_living_at_home,
    safe_cast(
        fixed_line_telephone_accommodation as string
    ) fixed_line_telephone_accommodation,
    safe_cast(
        personally_have_mobile_telephone as string
    ) personally_have_mobile_telephone,
    safe_cast(
        use_internet_telephone_calls_home as string
    ) use_internet_telephone_calls_home,
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
    safe_cast(region_austria as string) region_austria,
    safe_cast(region_belgium as string) region_belgium,
    safe_cast(region_bulgaria as string) region_bulgaria,
    safe_cast(region_switzerland as string) region_switzerland,
    safe_cast(region_cyprus as string) region_cyprus,
    safe_cast(region_germany as string) region_germany,
    safe_cast(region_denmark_2 as string) region_denmark_2,
    safe_cast(region_estonia as string) region_estonia,
    safe_cast(region_spain as string) region_spain,
    safe_cast(region_finland as string) region_finland,
    safe_cast(region_france as string) region_france,
    safe_cast(region_united_kingdom as string) region_united_kingdom,
    safe_cast(region_hungary as string) region_hungary,
    safe_cast(region_ireland_2 as string) region_ireland_2,
    safe_cast(region_latvia as string) region_latvia,
    safe_cast(region_netherlands as string) region_netherlands,
    safe_cast(region_norway as string) region_norway,
    safe_cast(region_poland as string) region_poland,
    safe_cast(region_portugal_2 as string) region_portugal_2,
    safe_cast(region_romania as string) region_romania,
    safe_cast(region_russian_federation as string) region_russian_federation,
    safe_cast(region_sweden as string) region_sweden,
    safe_cast(region_slovenia as string) region_slovenia,
    safe_cast(region_slovakia as string) region_slovakia,
    safe_cast(region_ukraine as string) region_ukraine,
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
        interview_length_minutes_main_questionnaire as int64
    ) interview_length_minutes_main_questionnaire,
    safe_cast(
        administration_split_ballot_mtmm_5 as string
    ) administration_split_ballot_mtmm_5,
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
    safe_cast(design_weight as float64) design_weight,
    safe_cast(poststratification_weight as float64) poststratification_weight,
    safe_cast(population_size_weight as float64) population_size_weight
from {{ set_datalake_project("gb_eric_ess_staging.round_03") }} as t
