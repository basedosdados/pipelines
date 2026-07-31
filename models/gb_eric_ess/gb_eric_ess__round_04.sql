{{
    config(
        schema="gb_eric_ess",
        alias="round_04",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2013, "interval": 1},
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
    safe_cast(
        politics_too_complicated_understand as string
    ) politics_too_complicated_understand,
    safe_cast(
        making_mind_up_political_issues as string
    ) making_mind_up_political_issues,
    safe_cast(trust_legal_system as int64) trust_legal_system,
    safe_cast(trust_police as int64) trust_police,
    safe_cast(trust_politicians as int64) trust_politicians,
    safe_cast(trust_european_parliament as int64) trust_european_parliament,
    safe_cast(trust_united_nations as int64) trust_united_nations,
    safe_cast(trust_political_parties as int64) trust_political_parties,
    safe_cast(trust_parliament as int64) trust_parliament,
    safe_cast(voted_last_national_election as string) voted_last_national_election,
    safe_cast(party_voted_austria_4 as string) party_voted_austria_4,
    safe_cast(party_voted_belgium_4 as string) party_voted_belgium_4,
    safe_cast(party_voted_bulgaria_5 as string) party_voted_bulgaria_5,
    safe_cast(party_voted_switzerland_7 as string) party_voted_switzerland_7,
    safe_cast(party_voted_cyprus_4 as string) party_voted_cyprus_4,
    safe_cast(party_voted_czechia_5 as string) party_voted_czechia_5,
    safe_cast(party_voted_1_germany_5 as string) party_voted_1_germany_5,
    safe_cast(party_voted_2_germany_5 as string) party_voted_2_germany_5,
    safe_cast(party_voted_denmark_3 as string) party_voted_denmark_3,
    safe_cast(party_voted_estonia_7 as string) party_voted_estonia_7,
    safe_cast(party_voted_spain_5 as string) party_voted_spain_5,
    safe_cast(party_voted_finland_6 as string) party_voted_finland_6,
    safe_cast(party_voted_france_ballot_1_5 as string) party_voted_france_ballot_1_5,
    safe_cast(party_voted_united_kingdom_4 as string) party_voted_united_kingdom_4,
    safe_cast(party_voted_greece_4 as string) party_voted_greece_4,
    safe_cast(party_voted_croatia_4 as string) party_voted_croatia_4,
    safe_cast(party_voted_hungary_7 as string) party_voted_hungary_7,
    safe_cast(party_voted_ireland_3 as string) party_voted_ireland_3,
    safe_cast(party_voted_israel_4 as string) party_voted_israel_4,
    safe_cast(
        party_voted_1_lithuania_first_vote_party_4 as string
    ) party_voted_1_lithuania_first_vote_party_4,
    safe_cast(
        party_voted_2_lithuania_second_vote_party_4 as string
    ) party_voted_2_lithuania_second_vote_party_4,
    safe_cast(
        party_voted_3_lithuania_third_vote_party_4 as string
    ) party_voted_3_lithuania_third_vote_party_4,
    safe_cast(party_voted_latvia_3 as string) party_voted_latvia_3,
    safe_cast(party_voted_netherlands_7 as string) party_voted_netherlands_7,
    safe_cast(party_voted_norway_4 as string) party_voted_norway_4,
    safe_cast(party_voted_poland_4 as string) party_voted_poland_4,
    safe_cast(party_voted_portugal_5 as string) party_voted_portugal_5,
    safe_cast(party_voted_romania as string) party_voted_romania,
    safe_cast(
        party_voted_russian_federation_4 as string
    ) party_voted_russian_federation_4,
    safe_cast(party_voted_sweden_5 as string) party_voted_sweden_5,
    safe_cast(party_voted_slovenia_5 as string) party_voted_slovenia_5,
    safe_cast(party_voted_slovakia_5 as string) party_voted_slovakia_5,
    safe_cast(party_voted_turkey as string) party_voted_turkey,
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
    safe_cast(party_feel_close_austria_4 as string) party_feel_close_austria_4,
    safe_cast(party_feel_close_belgium_4 as string) party_feel_close_belgium_4,
    safe_cast(party_feel_close_bulgaria_6 as string) party_feel_close_bulgaria_6,
    safe_cast(party_feel_close_switzerland_7 as string) party_feel_close_switzerland_7,
    safe_cast(party_feel_close_cyprus_4 as string) party_feel_close_cyprus_4,
    safe_cast(party_feel_close_czechia_5 as string) party_feel_close_czechia_5,
    safe_cast(party_feel_close_germany_5 as string) party_feel_close_germany_5,
    safe_cast(party_feel_close_denmark_3 as string) party_feel_close_denmark_3,
    safe_cast(party_feel_close_estonia_7 as string) party_feel_close_estonia_7,
    safe_cast(party_feel_close_spain_6 as string) party_feel_close_spain_6,
    safe_cast(party_feel_close_finland_7 as string) party_feel_close_finland_7,
    safe_cast(party_feel_close_france_6 as string) party_feel_close_france_6,
    safe_cast(
        party_feel_close_united_kingdom_4 as string
    ) party_feel_close_united_kingdom_4,
    safe_cast(party_feel_close_greece_4 as string) party_feel_close_greece_4,
    safe_cast(party_feel_close_croatia_3 as string) party_feel_close_croatia_3,
    safe_cast(party_feel_close_hungary_8 as string) party_feel_close_hungary_8,
    safe_cast(party_feel_close_ireland_5 as string) party_feel_close_ireland_5,
    safe_cast(party_feel_close_israel_5 as string) party_feel_close_israel_5,
    safe_cast(party_feel_close_lithuania_4 as string) party_feel_close_lithuania_4,
    safe_cast(party_feel_close_latvia_3 as string) party_feel_close_latvia_3,
    safe_cast(party_feel_close_netherlands_7 as string) party_feel_close_netherlands_7,
    safe_cast(party_feel_close_norway_4 as string) party_feel_close_norway_4,
    safe_cast(party_feel_close_poland_7 as string) party_feel_close_poland_7,
    safe_cast(party_feel_close_portugal_6 as string) party_feel_close_portugal_6,
    safe_cast(party_feel_close_romania as string) party_feel_close_romania,
    safe_cast(
        party_feel_close_russian_federation_4 as string
    ) party_feel_close_russian_federation_4,
    safe_cast(party_feel_close_sweden_5 as string) party_feel_close_sweden_5,
    safe_cast(party_feel_close_slovenia_5 as string) party_feel_close_slovenia_5,
    safe_cast(party_feel_close_slovakia_5 as string) party_feel_close_slovakia_5,
    safe_cast(party_feel_close_turkey as string) party_feel_close_turkey,
    safe_cast(party_feel_close_ukraine_3 as string) party_feel_close_ukraine_3,
    safe_cast(how_close_to_party as string) how_close_to_party,
    safe_cast(member_political_party as string) member_political_party,
    safe_cast(member_party_austria as string) member_party_austria,
    safe_cast(member_party_belgium_2 as string) member_party_belgium_2,
    safe_cast(member_party_bulgaria_2 as string) member_party_bulgaria_2,
    safe_cast(member_party_switzerland_2 as string) member_party_switzerland_2,
    safe_cast(member_party_cyprus as string) member_party_cyprus,
    safe_cast(member_party_czechia_2 as string) member_party_czechia_2,
    safe_cast(member_party_germany_2 as string) member_party_germany_2,
    safe_cast(member_party_denmark as string) member_party_denmark,
    safe_cast(member_party_estonia_2 as string) member_party_estonia_2,
    safe_cast(member_party_spain as string) member_party_spain,
    safe_cast(member_party_finland_2 as string) member_party_finland_2,
    safe_cast(member_party_france_2 as string) member_party_france_2,
    safe_cast(member_party_united_kingdom as string) member_party_united_kingdom,
    safe_cast(member_party_greece_2 as string) member_party_greece_2,
    safe_cast(member_party_croatia as string) member_party_croatia,
    safe_cast(member_party_hungary_2 as string) member_party_hungary_2,
    safe_cast(member_party_ireland_2 as string) member_party_ireland_2,
    safe_cast(member_party_israel_2 as string) member_party_israel_2,
    safe_cast(member_party_lithuania as string) member_party_lithuania,
    safe_cast(member_party_latvia as string) member_party_latvia,
    safe_cast(member_party_netherlands_2 as string) member_party_netherlands_2,
    safe_cast(member_party_norway_2 as string) member_party_norway_2,
    safe_cast(member_party_poland_2 as string) member_party_poland_2,
    safe_cast(member_party_portugal_2 as string) member_party_portugal_2,
    safe_cast(member_party_romania as string) member_party_romania,
    safe_cast(
        member_party_russian_federation_2 as string
    ) member_party_russian_federation_2,
    safe_cast(member_party_sweden_2 as string) member_party_sweden_2,
    safe_cast(member_party_slovenia as string) member_party_slovenia,
    safe_cast(member_party_slovakia_2 as string) member_party_slovakia_2,
    safe_cast(member_party_turkey as string) member_party_turkey,
    safe_cast(member_party_ukraine_2 as string) member_party_ukraine_2,
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
        religion_denomination_belonging_present_austria_2 as string
    ) religion_denomination_belonging_present_austria_2,
    safe_cast(
        religion_denomination_belonging_present_switzerland_2 as string
    ) religion_denomination_belonging_present_switzerland_2,
    safe_cast(
        religion_denomination_belonging_present_cyprus_2 as string
    ) religion_denomination_belonging_present_cyprus_2,
    safe_cast(
        religion_denomination_belonging_present_finland_2 as string
    ) religion_denomination_belonging_present_finland_2,
    safe_cast(
        religion_denomination_belonging_present_united_kingdom as string
    ) religion_denomination_belonging_present_united_kingdom,
    safe_cast(
        religion_denomination_belonging_present_greece_2 as string
    ) religion_denomination_belonging_present_greece_2,
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
        religion_denomination_belonging_present_latvia as string
    ) religion_denomination_belonging_present_latvia,
    safe_cast(
        religion_denomination_belonging_present_netherlands_2 as string
    ) religion_denomination_belonging_present_netherlands_2,
    safe_cast(
        religion_denomination_belonging_present_norway as string
    ) religion_denomination_belonging_present_norway,
    safe_cast(
        religion_denomination_belonging_present_poland_2 as string
    ) religion_denomination_belonging_present_poland_2,
    safe_cast(
        religion_denomination_belonging_present_portugal_2 as string
    ) religion_denomination_belonging_present_portugal_2,
    safe_cast(
        religion_denomination_belonging_present_romania as string
    ) religion_denomination_belonging_present_romania,
    safe_cast(
        religion_denomination_belonging_present_russian_federation_2 as string
    ) religion_denomination_belonging_present_russian_federation_2,
    safe_cast(
        religion_denomination_belonging_present_sweden_2 as string
    ) religion_denomination_belonging_present_sweden_2,
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
        religion_denomination_belonging_past_austria_2 as string
    ) religion_denomination_belonging_past_austria_2,
    safe_cast(
        religion_denomination_belonging_past_switzerland_2 as string
    ) religion_denomination_belonging_past_switzerland_2,
    safe_cast(
        religion_denomination_belonging_past_cyprus_2 as string
    ) religion_denomination_belonging_past_cyprus_2,
    safe_cast(
        religion_denomination_belonging_past_finland_2 as string
    ) religion_denomination_belonging_past_finland_2,
    safe_cast(
        religion_denomination_belonging_past_united_kingdom as string
    ) religion_denomination_belonging_past_united_kingdom,
    safe_cast(
        religion_denomination_belonging_past_greece_2 as string
    ) religion_denomination_belonging_past_greece_2,
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
        religion_denomination_belonging_past_latvia as string
    ) religion_denomination_belonging_past_latvia,
    safe_cast(
        religion_denomination_belonging_past_netherlands_2 as string
    ) religion_denomination_belonging_past_netherlands_2,
    safe_cast(
        religion_denomination_belonging_past_norway as string
    ) religion_denomination_belonging_past_norway,
    safe_cast(
        religion_denomination_belonging_past_poland_2 as string
    ) religion_denomination_belonging_past_poland_2,
    safe_cast(
        religion_denomination_belonging_past_portugal_2 as string
    ) religion_denomination_belonging_past_portugal_2,
    safe_cast(
        religion_denomination_belonging_past_romania as string
    ) religion_denomination_belonging_past_romania,
    safe_cast(
        religion_denomination_belonging_past_russian_federation_2 as string
    ) religion_denomination_belonging_past_russian_federation_2,
    safe_cast(
        religion_denomination_belonging_past_sweden_2 as string
    ) religion_denomination_belonging_past_sweden_2,
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
    safe_cast(
        long_ago_first_came_live_country as string
    ) long_ago_first_came_live_country,
    safe_cast(language_home_first_mentioned as string) language_home_first_mentioned,
    safe_cast(language_home_second_mentioned as string) language_home_second_mentioned,
    safe_cast(belong_ethnic_minority as string) belong_ethnic_minority,
    safe_cast(father_born_in_country as string) father_born_in_country,
    safe_cast(country_birth_father_3 as string) country_birth_father_3,
    safe_cast(mother_born_in_country as string) mother_born_in_country,
    safe_cast(country_birth_mother_3 as string) country_birth_mother_3,
    safe_cast(
        large_differences_income_acceptable_reward_talents as string
    ) large_differences_income_acceptable_reward_talents,
    safe_cast(
        schools_teach_children_obey_authority as string
    ) schools_teach_children_obey_authority,
    safe_cast(
        women_should_prepared_cut_down_paid as string
    ) women_should_prepared_cut_down_paid,
    safe_cast(
        fair_society_differences_standard_living_should as string
    ) fair_society_differences_standard_living_should,
    safe_cast(
        people_who_break_law_much_harsher_sentences_2 as string
    ) people_who_break_law_much_harsher_sentences_2,
    safe_cast(men_should_have_more_right_job as string) men_should_have_more_right_job,
    safe_cast(
        every_100_working_age_number_unemployed as string
    ) every_100_working_age_number_unemployed,
    safe_cast(
        every_100_working_age_number_long as string
    ) every_100_working_age_number_long,
    safe_cast(
        every_100_working_age_number_not as string
    ) every_100_working_age_number_not,
    safe_cast(
        every_100_working_age_number_born as string
    ) every_100_working_age_number_born,
    safe_cast(standard_living_pensioners as int64) standard_living_pensioners,
    safe_cast(standard_living_unemployed as int64) standard_living_unemployed,
    safe_cast(
        provision_affordable_child_care_services_working as int64
    ) provision_affordable_child_care_services_working,
    safe_cast(
        opportunities_young_people_find_first_full as int64
    ) opportunities_young_people_find_first_full,
    safe_cast(
        job_everyone_governments_responsibility as int64
    ) job_everyone_governments_responsibility,
    safe_cast(
        health_care_sick_governments_responsibility as int64
    ) health_care_sick_governments_responsibility,
    safe_cast(
        standard_living_old_governments_responsibility as int64
    ) standard_living_old_governments_responsibility,
    safe_cast(
        standard_living_unemployed_governments_responsibility as int64
    ) standard_living_unemployed_governments_responsibility,
    safe_cast(
        child_care_services_working_parents_governments as int64
    ) child_care_services_working_parents_governments,
    safe_cast(paid_leave_from_work_care_sick as int64) paid_leave_from_work_care_sick,
    safe_cast(
        social_benefits_services_place_too_great as string
    ) social_benefits_services_place_too_great,
    safe_cast(
        social_benefits_services_prevent_widespread_poverty as string
    ) social_benefits_services_prevent_widespread_poverty,
    safe_cast(
        social_benefits_services_lead_more_equal_society as string
    ) social_benefits_services_lead_more_equal_society,
    safe_cast(
        social_benefits_services_encourage_people_other as string
    ) social_benefits_services_encourage_people_other,
    safe_cast(
        social_benefits_services_cost_businesses_too as string
    ) social_benefits_services_cost_businesses_too,
    safe_cast(
        social_benefits_services_make_it_easier as string
    ) social_benefits_services_make_it_easier,
    safe_cast(
        social_benefits_services_make_people_lazy as string
    ) social_benefits_services_make_people_lazy,
    safe_cast(
        social_benefits_services_make_people_less as string
    ) social_benefits_services_make_people_less,
    safe_cast(
        social_benefits_services_make_people_less_2 as string
    ) social_benefits_services_make_people_less_2,
    safe_cast(provision_health_care_efficient as int64) provision_health_care_efficient,
    safe_cast(
        tax_authorities_efficient_doing_their_job as int64
    ) tax_authorities_efficient_doing_their_job,
    safe_cast(
        doctors_nurses_give_special_advantages_deal as int64
    ) doctors_nurses_give_special_advantages_deal,
    safe_cast(
        tax_authorities_give_special_advantages_deal as int64
    ) tax_authorities_give_special_advantages_deal,
    safe_cast(
        government_decrease_increase_taxes_social_spending as int64
    ) government_decrease_increase_taxes_social_spending,
    safe_cast(
        taxation_higher_versus_lower_earners as string
    ) taxation_higher_versus_lower_earners,
    safe_cast(
        higher_lower_earners_should_get_larger as string
    ) higher_lower_earners_should_get_larger,
    safe_cast(
        higher_lower_earners_should_get_larger_2 as string
    ) higher_lower_earners_should_get_larger_2,
    safe_cast(
        when_should_immigrants_obtain_rights_social as string
    ) when_should_immigrants_obtain_rights_social,
    safe_cast(
        immigrants_receive_more_less_than_they as int64
    ) immigrants_receive_more_less_than_they,
    safe_cast(
        most_unemployed_people_do_not_really as string
    ) most_unemployed_people_do_not_really,
    safe_cast(many_very_low_incomes_get_less as string) many_very_low_incomes_get_less,
    safe_cast(
        many_manage_obtain_benefits_services_not as string
    ) many_manage_obtain_benefits_services_not,
    safe_cast(
        insufficient_benefits_country_help_people_real as string
    ) insufficient_benefits_country_help_people_real,
    safe_cast(
        employees_often_pretend_they_sick_stay_home as string
    ) employees_often_pretend_they_sick_stay_home,
    safe_cast(
        level_public_health_care_affordable_10 as string
    ) level_public_health_care_affordable_10,
    safe_cast(
        level_old_age_pension_affordable_10 as string
    ) level_old_age_pension_affordable_10,
    safe_cast(
        likelihood_unemployed_looking_work_next_12 as string
    ) likelihood_unemployed_looking_work_next_12,
    safe_cast(
        likelihood_less_time_paid_work_than as string
    ) likelihood_less_time_paid_work_than,
    safe_cast(
        likelihood_not_enough_money_household_necessities as string
    ) likelihood_not_enough_money_household_necessities,
    safe_cast(
        likelihood_not_receive_health_care_needed as string
    ) likelihood_not_receive_health_care_needed,
    safe_cast(
        age_people_stop_described_young as string
    ) age_people_stop_described_young,
    safe_cast(age_people_start_described_old as string) age_people_start_described_old,
    safe_cast(age_group_belonging as string) age_group_belonging,
    safe_cast(
        strong_weak_sense_belonging_age_group as int64
    ) strong_weak_sense_belonging_age_group,
    safe_cast(
        most_people_view_status_people_their_20s as int64
    ) most_people_view_status_people_their_20s,
    safe_cast(
        most_people_view_status_people_their_40s as int64
    ) most_people_view_status_people_their_40s,
    safe_cast(
        most_people_view_status_people_over_70 as int64
    ) most_people_view_status_people_over_70,
    safe_cast(
        worried_level_crime_committed_people_their_20s as int64
    ) worried_level_crime_committed_people_their_20s,
    safe_cast(
        worried_employers_prefer_people_20s_rather as int64
    ) worried_employers_prefer_people_20s_rather,
    safe_cast(
        people_their_20s_effect_customs_way_life as int64
    ) people_their_20s_effect_customs_way_life,
    safe_cast(
        people_their_20s_contribution_economy_these_days as int64
    ) people_their_20s_contribution_economy_these_days,
    safe_cast(
        people_over_70_burden_health_service_these_days as int64
    ) people_over_70_burden_health_service_these_days,
    safe_cast(
        people_over_70_effect_customs_way_life as int64
    ) people_over_70_effect_customs_way_life,
    safe_cast(
        people_over_70_contribution_economy_these_days as int64
    ) people_over_70_contribution_economy_these_days,
    safe_cast(
        most_people_view_those_their_20s_friendly as string
    ) most_people_view_those_their_20s_friendly,
    safe_cast(
        most_people_view_those_their_20s_competent as string
    ) most_people_view_those_their_20s_competent,
    safe_cast(
        most_people_view_those_their_20s as string
    ) most_people_view_those_their_20s,
    safe_cast(
        most_people_view_those_their_20s_respect as string
    ) most_people_view_those_their_20s_respect,
    safe_cast(
        most_people_view_those_over_70_friendly as string
    ) most_people_view_those_over_70_friendly,
    safe_cast(
        most_people_view_those_over_70_competent as string
    ) most_people_view_those_over_70_competent,
    safe_cast(most_people_view_those_over_70 as string) most_people_view_those_over_70,
    safe_cast(
        most_people_view_those_over_70_respect as string
    ) most_people_view_those_over_70_respect,
    safe_cast(
        acceptable_most_people_if_qualified_30 as int64
    ) acceptable_most_people_if_qualified_30,
    safe_cast(
        acceptable_most_people_if_qualified_70 as int64
    ) acceptable_most_people_if_qualified_70,
    safe_cast(
        most_people_view_those_their_20s_envy as string
    ) most_people_view_those_their_20s_envy,
    safe_cast(
        most_people_view_those_their_20s_pity as string
    ) most_people_view_those_their_20s_pity,
    safe_cast(
        most_people_view_those_their_20s_admiration as string
    ) most_people_view_those_their_20s_admiration,
    safe_cast(
        most_people_view_those_their_20s_contempt as string
    ) most_people_view_those_their_20s_contempt,
    safe_cast(
        most_people_view_those_over_70_envy as string
    ) most_people_view_those_over_70_envy,
    safe_cast(
        most_people_view_those_over_70_pity as string
    ) most_people_view_those_over_70_pity,
    safe_cast(
        most_people_view_those_over_70_admiration as string
    ) most_people_view_those_over_70_admiration,
    safe_cast(
        most_people_view_those_over_70_contempt as string
    ) most_people_view_those_over_70_contempt,
    safe_cast(
        overall_negative_positive_feel_towards_people as int64
    ) overall_negative_positive_feel_towards_people,
    safe_cast(
        overall_negative_positive_feel_towards_people_2 as int64
    ) overall_negative_positive_feel_towards_people_2,
    safe_cast(
        frequency_past_year_treated_prejudice_because as string
    ) frequency_past_year_treated_prejudice_because,
    safe_cast(
        frequency_past_year_treated_prejudice_because_2 as string
    ) frequency_past_year_treated_prejudice_because_2,
    safe_cast(
        frequency_past_year_treated_prejudice_because_3 as string
    ) frequency_past_year_treated_prejudice_because_3,
    safe_cast(
        frequency_past_year_felt_lack_respect as string
    ) frequency_past_year_felt_lack_respect,
    safe_cast(
        frequency_past_year_treated_badly_because_age as string
    ) frequency_past_year_treated_badly_because_age,
    safe_cast(
        number_friends_other_than_family_younger_than_30 as string
    ) number_friends_other_than_family_younger_than_30,
    safe_cast(
        can_discuss_personal_issues_friends_younger as string
    ) can_discuss_personal_issues_friends_younger,
    safe_cast(
        number_friends_other_than_family_aged_over_70 as string
    ) number_friends_other_than_family_aged_over_70,
    safe_cast(
        can_discuss_personal_issues_friends_aged_over_70 as string
    ) can_discuss_personal_issues_friends_aged_over_70,
    safe_cast(
        respondent_age_younger_older_than_30 as string
    ) respondent_age_younger_older_than_30,
    safe_cast(
        any_children_grandchildren_between_age_15_30 as string
    ) any_children_grandchildren_between_age_15_30,
    safe_cast(
        can_discuss_personal_issues_children_grandchildren as string
    ) can_discuss_personal_issues_children_grandchildren,
    safe_cast(
        any_members_family_aged_over_70 as string
    ) any_members_family_aged_over_70,
    safe_cast(
        can_discuss_personal_issues_members_family as string
    ) can_discuss_personal_issues_members_family,
    safe_cast(done_paid_voluntary_work_month as string) done_paid_voluntary_work_month,
    safe_cast(
        time_spent_working_colleagues_their_20s_month as string
    ) time_spent_working_colleagues_their_20s_month,
    safe_cast(
        time_spent_working_colleagues_aged_over_70_month as string
    ) time_spent_working_colleagues_aged_over_70_month,
    safe_cast(see_people_their_20s_over_70 as string) see_people_their_20s_over_70,
    safe_cast(
        important_unprejudiced_against_other_age_groups as int64
    ) important_unprejudiced_against_other_age_groups,
    safe_cast(
        important_seen_unprejudiced_against_other_age as int64
    ) important_seen_unprejudiced_against_other_age,
    safe_cast(
        serious_discrimination_against_people_because_age as string
    ) serious_discrimination_against_people_because_age,
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
    safe_cast(household_member_16_gender as string) household_member_16_gender,
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
        household_member_15_year_of_birth as int64
    ) household_member_15_year_of_birth,
    safe_cast(
        household_member_16_year_of_birth as string
    ) household_member_16_year_of_birth,
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
    safe_cast(
        household_member_16_relationship as string
    ) household_member_16_relationship,
    safe_cast(domicile_type as string) domicile_type,
    safe_cast(highest_education_3 as string) highest_education_3,
    safe_cast(highest_education_es_isced as string) highest_education_es_isced,
    safe_cast(highest_education_austria_2 as string) highest_education_austria_2,
    safe_cast(highest_education_belgium_3 as string) highest_education_belgium_3,
    safe_cast(
        highest_education_switzerland_2 as string
    ) highest_education_switzerland_2,
    safe_cast(highest_education_cyprus_4 as string) highest_education_cyprus_4,
    safe_cast(highest_education_czechia_2 as string) highest_education_czechia_2,
    safe_cast(highest_education_denmark_2 as string) highest_education_denmark_2,
    safe_cast(highest_education_estonia_2 as string) highest_education_estonia_2,
    safe_cast(highest_education_spain_5 as string) highest_education_spain_5,
    safe_cast(highest_education_france_2 as string) highest_education_france_2,
    safe_cast(
        highest_education_united_kingdom_8 as string
    ) highest_education_united_kingdom_8,
    safe_cast(highest_education_greece_3 as string) highest_education_greece_3,
    safe_cast(highest_education_croatia_3 as string) highest_education_croatia_3,
    safe_cast(highest_education_hungary_3 as string) highest_education_hungary_3,
    safe_cast(highest_education_ireland_2 as string) highest_education_ireland_2,
    safe_cast(highest_education_israel as int64) highest_education_israel,
    safe_cast(highest_education_lithuania_2 as string) highest_education_lithuania_2,
    safe_cast(highest_education_latvia_3 as string) highest_education_latvia_3,
    safe_cast(
        highest_education_netherlands_3 as string
    ) highest_education_netherlands_3,
    safe_cast(highest_education_norway_3 as string) highest_education_norway_3,
    safe_cast(highest_education_poland_5 as string) highest_education_poland_5,
    safe_cast(highest_education_portugal_3 as string) highest_education_portugal_3,
    safe_cast(highest_education_romania as string) highest_education_romania,
    safe_cast(
        highest_education_russian_federation_2 as string
    ) highest_education_russian_federation_2,
    safe_cast(highest_education_sweden_2 as string) highest_education_sweden_2,
    safe_cast(highest_education_slovenia_3 as string) highest_education_slovenia_3,
    safe_cast(highest_education_slovakia_2 as string) highest_education_slovakia_2,
    safe_cast(highest_education_turkey as string) highest_education_turkey,
    safe_cast(highest_education_ukraine_2 as int64) highest_education_ukraine_2,
    safe_cast(
        field_subject_highest_qualification as string
    ) field_subject_highest_qualification,
    safe_cast(years_of_education as int64) years_of_education,
    safe_cast(activity_dont_know as string) activity_dont_know,
    safe_cast(activity_refusal as string) activity_refusal,
    safe_cast(activity_no_answer as string) activity_no_answer,
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
    safe_cast(industry_nace_rev_1_1 as string) industry_nace_rev_1_1,
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
        lives_husband_wife_partner_household_grid as string
    ) lives_husband_wife_partner_household_grid,
    safe_cast(partner_highest_education_3 as string) partner_highest_education_3,
    safe_cast(
        partner_highest_education_es_isced as string
    ) partner_highest_education_es_isced,
    safe_cast(
        partner_highest_education_austria_3 as string
    ) partner_highest_education_austria_3,
    safe_cast(
        partner_highest_education_belgium_3 as string
    ) partner_highest_education_belgium_3,
    safe_cast(
        partner_highest_education_switzerland_2 as string
    ) partner_highest_education_switzerland_2,
    safe_cast(
        partner_highest_education_cyprus_4 as string
    ) partner_highest_education_cyprus_4,
    safe_cast(
        partner_highest_education_czechia_2 as string
    ) partner_highest_education_czechia_2,
    safe_cast(
        partner_highest_education_denmark_2 as string
    ) partner_highest_education_denmark_2,
    safe_cast(
        partner_highest_education_estonia_2 as string
    ) partner_highest_education_estonia_2,
    safe_cast(
        partner_highest_education_spain_5 as string
    ) partner_highest_education_spain_5,
    safe_cast(
        partner_highest_education_france_2 as string
    ) partner_highest_education_france_2,
    safe_cast(
        partner_highest_education_united_kingdom_8 as string
    ) partner_highest_education_united_kingdom_8,
    safe_cast(
        partner_highest_education_greece_3 as string
    ) partner_highest_education_greece_3,
    safe_cast(
        partner_highest_education_croatia_3 as string
    ) partner_highest_education_croatia_3,
    safe_cast(
        partner_highest_education_hungary_3 as string
    ) partner_highest_education_hungary_3,
    safe_cast(
        partner_highest_education_ireland_2 as string
    ) partner_highest_education_ireland_2,
    safe_cast(
        partner_highest_education_israel as int64
    ) partner_highest_education_israel,
    safe_cast(
        partner_highest_education_lithuania_2 as string
    ) partner_highest_education_lithuania_2,
    safe_cast(
        partner_highest_education_latvia_3 as string
    ) partner_highest_education_latvia_3,
    safe_cast(
        partner_highest_education_netherlands_3 as string
    ) partner_highest_education_netherlands_3,
    safe_cast(
        partner_highest_education_norway_3 as string
    ) partner_highest_education_norway_3,
    safe_cast(
        partner_highest_education_poland_5 as string
    ) partner_highest_education_poland_5,
    safe_cast(
        partner_highest_education_portugal_3 as string
    ) partner_highest_education_portugal_3,
    safe_cast(
        partner_highest_education_romania as string
    ) partner_highest_education_romania,
    safe_cast(
        partner_highest_education_russian_federation_2 as string
    ) partner_highest_education_russian_federation_2,
    safe_cast(
        partner_highest_education_sweden_2 as string
    ) partner_highest_education_sweden_2,
    safe_cast(
        partner_highest_education_slovenia_3 as string
    ) partner_highest_education_slovenia_3,
    safe_cast(
        partner_highest_education_slovakia_2 as string
    ) partner_highest_education_slovakia_2,
    safe_cast(
        partner_highest_education_turkey as string
    ) partner_highest_education_turkey,
    safe_cast(
        partner_highest_education_ukraine_2 as int64
    ) partner_highest_education_ukraine_2,
    safe_cast(partner_activity_dont_know as string) partner_activity_dont_know,
    safe_cast(partner_activity_refusal as string) partner_activity_refusal,
    safe_cast(
        partner_activity_not_applicable as string
    ) partner_activity_not_applicable,
    safe_cast(partner_activity_no_answer as string) partner_activity_no_answer,
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
        partner_allowed_influence_policy_decisions_activities as int64
    ) partner_allowed_influence_policy_decisions_activities,
    safe_cast(
        partner_allowed_decide_daily_work_organised as int64
    ) partner_allowed_decide_daily_work_organised,
    safe_cast(
        hours_normally_worked_week_main_job as int64
    ) hours_normally_worked_week_main_job,
    safe_cast(father_highest_education_3 as string) father_highest_education_3,
    safe_cast(
        father_highest_education_es_isced as string
    ) father_highest_education_es_isced,
    safe_cast(
        father_highest_education_austria_2 as string
    ) father_highest_education_austria_2,
    safe_cast(
        father_highest_education_belgium_3 as string
    ) father_highest_education_belgium_3,
    safe_cast(
        father_highest_education_switzerland_2 as string
    ) father_highest_education_switzerland_2,
    safe_cast(
        father_highest_education_cyprus_4 as string
    ) father_highest_education_cyprus_4,
    safe_cast(
        father_highest_education_czechia_2 as string
    ) father_highest_education_czechia_2,
    safe_cast(
        father_highest_education_denmark_2 as string
    ) father_highest_education_denmark_2,
    safe_cast(
        father_highest_education_estonia_2 as string
    ) father_highest_education_estonia_2,
    safe_cast(
        father_highest_education_spain_5 as string
    ) father_highest_education_spain_5,
    safe_cast(
        father_highest_education_france_2 as string
    ) father_highest_education_france_2,
    safe_cast(
        father_highest_education_united_kingdom_8 as string
    ) father_highest_education_united_kingdom_8,
    safe_cast(
        father_highest_education_greece_3 as string
    ) father_highest_education_greece_3,
    safe_cast(
        father_highest_education_croatia_3 as string
    ) father_highest_education_croatia_3,
    safe_cast(
        father_highest_education_hungary_3 as string
    ) father_highest_education_hungary_3,
    safe_cast(
        father_highest_education_ireland_2 as string
    ) father_highest_education_ireland_2,
    safe_cast(father_highest_education_israel as int64) father_highest_education_israel,
    safe_cast(
        father_highest_education_lithuania_2 as string
    ) father_highest_education_lithuania_2,
    safe_cast(
        father_highest_education_latvia_3 as string
    ) father_highest_education_latvia_3,
    safe_cast(
        father_highest_education_netherlands_3 as string
    ) father_highest_education_netherlands_3,
    safe_cast(
        father_highest_education_norway_3 as string
    ) father_highest_education_norway_3,
    safe_cast(
        father_highest_education_poland_5 as string
    ) father_highest_education_poland_5,
    safe_cast(
        father_highest_education_portugal_3 as string
    ) father_highest_education_portugal_3,
    safe_cast(
        father_highest_education_romania as string
    ) father_highest_education_romania,
    safe_cast(
        father_highest_education_russian_federation_2 as string
    ) father_highest_education_russian_federation_2,
    safe_cast(
        father_highest_education_sweden_2 as string
    ) father_highest_education_sweden_2,
    safe_cast(
        father_highest_education_slovenia_3 as string
    ) father_highest_education_slovenia_3,
    safe_cast(
        father_highest_education_slovakia_2 as string
    ) father_highest_education_slovakia_2,
    safe_cast(
        father_highest_education_turkey as string
    ) father_highest_education_turkey,
    safe_cast(
        father_highest_education_ukraine_2 as int64
    ) father_highest_education_ukraine_2,
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
    safe_cast(mother_highest_education_3 as string) mother_highest_education_3,
    safe_cast(
        mother_highest_education_es_isced as string
    ) mother_highest_education_es_isced,
    safe_cast(
        mother_highest_education_austria_2 as string
    ) mother_highest_education_austria_2,
    safe_cast(
        mother_highest_education_belgium_3 as string
    ) mother_highest_education_belgium_3,
    safe_cast(
        mother_highest_education_switzerland_2 as string
    ) mother_highest_education_switzerland_2,
    safe_cast(
        mother_highest_education_cyprus_4 as string
    ) mother_highest_education_cyprus_4,
    safe_cast(
        mother_highest_education_czechia_2 as string
    ) mother_highest_education_czechia_2,
    safe_cast(
        mother_highest_education_denmark_2 as string
    ) mother_highest_education_denmark_2,
    safe_cast(
        mother_highest_education_estonia_2 as string
    ) mother_highest_education_estonia_2,
    safe_cast(
        mother_highest_education_spain_5 as string
    ) mother_highest_education_spain_5,
    safe_cast(
        mother_highest_education_france_2 as string
    ) mother_highest_education_france_2,
    safe_cast(
        mother_highest_education_united_kingdom_8 as string
    ) mother_highest_education_united_kingdom_8,
    safe_cast(
        mother_highest_education_greece_3 as string
    ) mother_highest_education_greece_3,
    safe_cast(
        mother_highest_education_croatia_3 as string
    ) mother_highest_education_croatia_3,
    safe_cast(
        mother_highest_education_hungary_3 as string
    ) mother_highest_education_hungary_3,
    safe_cast(
        mother_highest_education_ireland_2 as string
    ) mother_highest_education_ireland_2,
    safe_cast(mother_highest_education_israel as int64) mother_highest_education_israel,
    safe_cast(
        mother_highest_education_lithuania_2 as string
    ) mother_highest_education_lithuania_2,
    safe_cast(
        mother_highest_education_latvia_3 as string
    ) mother_highest_education_latvia_3,
    safe_cast(
        mother_highest_education_netherlands_3 as string
    ) mother_highest_education_netherlands_3,
    safe_cast(
        mother_highest_education_norway_3 as string
    ) mother_highest_education_norway_3,
    safe_cast(
        mother_highest_education_poland_5 as string
    ) mother_highest_education_poland_5,
    safe_cast(
        mother_highest_education_portugal_3 as string
    ) mother_highest_education_portugal_3,
    safe_cast(
        mother_highest_education_romania as string
    ) mother_highest_education_romania,
    safe_cast(
        mother_highest_education_russian_federation_2 as string
    ) mother_highest_education_russian_federation_2,
    safe_cast(
        mother_highest_education_sweden_2 as string
    ) mother_highest_education_sweden_2,
    safe_cast(
        mother_highest_education_slovenia_3 as string
    ) mother_highest_education_slovenia_3,
    safe_cast(
        mother_highest_education_slovakia_2 as string
    ) mother_highest_education_slovakia_2,
    safe_cast(
        mother_highest_education_turkey as string
    ) mother_highest_education_turkey,
    safe_cast(
        mother_highest_education_ukraine_2 as int64
    ) mother_highest_education_ukraine_2,
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
    safe_cast(region_austria as string) region_austria,
    safe_cast(region_belgium as string) region_belgium,
    safe_cast(region_bulgaria as string) region_bulgaria,
    safe_cast(region_switzerland as string) region_switzerland,
    safe_cast(region_cyprus as string) region_cyprus,
    safe_cast(region_czechia as string) region_czechia,
    safe_cast(region_germany as string) region_germany,
    safe_cast(region_denmark as string) region_denmark,
    safe_cast(region_estonia as string) region_estonia,
    safe_cast(region_spain as string) region_spain,
    safe_cast(region_finland as string) region_finland,
    safe_cast(region_france as string) region_france,
    safe_cast(region_united_kingdom as string) region_united_kingdom,
    safe_cast(region_greece as string) region_greece,
    safe_cast(region_croatia as string) region_croatia,
    safe_cast(region_hungary as string) region_hungary,
    safe_cast(region_ireland as string) region_ireland,
    safe_cast(region_israel as string) region_israel,
    safe_cast(region_lithuania as string) region_lithuania,
    safe_cast(region_latvia as string) region_latvia,
    safe_cast(region_netherlands as string) region_netherlands,
    safe_cast(region_norway as string) region_norway,
    safe_cast(region_poland as string) region_poland,
    safe_cast(region_portugal as string) region_portugal,
    safe_cast(region_romania as string) region_romania,
    safe_cast(region_russian_federation as string) region_russian_federation,
    safe_cast(region_sweden as string) region_sweden,
    safe_cast(region_slovenia as string) region_slovenia,
    safe_cast(region_slovakia as string) region_slovakia,
    safe_cast(region_turkey as string) region_turkey,
    safe_cast(region_ukraine as string) region_ukraine,
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
        administration_split_ballot_mtmm_4 as string
    ) administration_split_ballot_mtmm_4,
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
from {{ set_datalake_project("gb_eric_ess_staging.round_04") }} as t
