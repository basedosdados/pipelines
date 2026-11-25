{{
    config(
        schema="gb_eric_ess",
        alias="round_01",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2002, "end": 2007, "interval": 1},
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
        could_take_active_role_group_involved as string
    ) could_take_active_role_group_involved,
    safe_cast(
        making_mind_up_political_issues as string
    ) making_mind_up_political_issues,
    safe_cast(
        politicians_general_care_what_people_like as string
    ) politicians_general_care_what_people_like,
    safe_cast(
        politicians_interested_votes_rather_than_people as string
    ) politicians_interested_votes_rather_than_people,
    safe_cast(trust_legal_system as int64) trust_legal_system,
    safe_cast(trust_police as int64) trust_police,
    safe_cast(trust_politicians as int64) trust_politicians,
    safe_cast(trust_european_parliament as int64) trust_european_parliament,
    safe_cast(trust_united_nations as int64) trust_united_nations,
    safe_cast(trust_parliament as int64) trust_parliament,
    safe_cast(voted_last_national_election as string) voted_last_national_election,
    safe_cast(party_voted_austria_5 as string) party_voted_austria_5,
    safe_cast(party_voted_belgium_6 as string) party_voted_belgium_6,
    safe_cast(party_voted_switzerland_9 as string) party_voted_switzerland_9,
    safe_cast(party_voted_czechia_6 as string) party_voted_czechia_6,
    safe_cast(party_voted_1_germany_7 as string) party_voted_1_germany_7,
    safe_cast(party_voted_2_germany_7 as string) party_voted_2_germany_7,
    safe_cast(party_voted_denmark_5 as string) party_voted_denmark_5,
    safe_cast(party_voted_spain_7 as string) party_voted_spain_7,
    safe_cast(party_voted_finland_7 as string) party_voted_finland_7,
    safe_cast(party_voted_france_ballot_1_7 as string) party_voted_france_ballot_1_7,
    safe_cast(party_voted_united_kingdom_4 as string) party_voted_united_kingdom_4,
    safe_cast(party_voted_greece_6 as string) party_voted_greece_6,
    safe_cast(party_voted_hungary_9 as string) party_voted_hungary_9,
    safe_cast(party_voted_ireland_3 as string) party_voted_ireland_3,
    safe_cast(party_voted_israel_5 as string) party_voted_israel_5,
    safe_cast(party_voted_italy_5 as string) party_voted_italy_5,
    safe_cast(party_voted_luxembourg as string) party_voted_luxembourg,
    safe_cast(party_voted_netherlands_10 as string) party_voted_netherlands_10,
    safe_cast(party_voted_norway_4 as string) party_voted_norway_4,
    safe_cast(party_voted_poland_6 as string) party_voted_poland_6,
    safe_cast(party_voted_portugal_6 as string) party_voted_portugal_6,
    safe_cast(party_voted_sweden_5 as string) party_voted_sweden_5,
    safe_cast(party_voted_slovenia_8 as string) party_voted_slovenia_8,
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
    safe_cast(
        bought_product_political_ethical_environment_reason as string
    ) bought_product_political_ethical_environment_reason,
    safe_cast(
        donated_money_political_organisation_group_12 as string
    ) donated_money_political_organisation_group_12,
    safe_cast(
        participated_illegal_protest_activities_12_months as string
    ) participated_illegal_protest_activities_12_months,
    safe_cast(feel_close_to_party as string) feel_close_to_party,
    safe_cast(party_feel_close_austria_5 as string) party_feel_close_austria_5,
    safe_cast(party_feel_close_belgium_6 as string) party_feel_close_belgium_6,
    safe_cast(party_feel_close_switzerland_9 as string) party_feel_close_switzerland_9,
    safe_cast(party_feel_close_czechia_6 as string) party_feel_close_czechia_6,
    safe_cast(party_feel_close_germany_7 as string) party_feel_close_germany_7,
    safe_cast(party_feel_close_denmark_5 as string) party_feel_close_denmark_5,
    safe_cast(party_feel_close_spain_8 as string) party_feel_close_spain_8,
    safe_cast(party_feel_close_finland_8 as string) party_feel_close_finland_8,
    safe_cast(party_feel_close_france_8 as string) party_feel_close_france_8,
    safe_cast(
        party_feel_close_united_kingdom_4 as string
    ) party_feel_close_united_kingdom_4,
    safe_cast(party_feel_close_greece_6 as string) party_feel_close_greece_6,
    safe_cast(party_feel_close_hungary_10 as string) party_feel_close_hungary_10,
    safe_cast(party_feel_close_ireland_7 as string) party_feel_close_ireland_7,
    safe_cast(party_feel_close_israel_6 as string) party_feel_close_israel_6,
    safe_cast(party_feel_close_italy_6 as string) party_feel_close_italy_6,
    safe_cast(party_feel_close_luxembourg as string) party_feel_close_luxembourg,
    safe_cast(party_feel_close_netherlands_8 as string) party_feel_close_netherlands_8,
    safe_cast(party_feel_close_norway_4 as string) party_feel_close_norway_4,
    safe_cast(party_feel_close_poland_10 as string) party_feel_close_poland_10,
    safe_cast(party_feel_close_portugal_8 as string) party_feel_close_portugal_8,
    safe_cast(party_feel_close_sweden_5 as string) party_feel_close_sweden_5,
    safe_cast(party_feel_close_slovenia_8 as string) party_feel_close_slovenia_8,
    safe_cast(how_close_to_party as string) how_close_to_party,
    safe_cast(member_political_party as string) member_political_party,
    safe_cast(member_party_austria_2 as string) member_party_austria_2,
    safe_cast(member_party_belgium_4 as string) member_party_belgium_4,
    safe_cast(member_party_switzerland_4 as string) member_party_switzerland_4,
    safe_cast(member_party_czechia_3 as string) member_party_czechia_3,
    safe_cast(member_party_germany_4 as string) member_party_germany_4,
    safe_cast(member_party_denmark_3 as string) member_party_denmark_3,
    safe_cast(member_party_spain_3 as string) member_party_spain_3,
    safe_cast(member_party_finland_3 as string) member_party_finland_3,
    safe_cast(member_party_france_4 as string) member_party_france_4,
    safe_cast(member_party_united_kingdom as string) member_party_united_kingdom,
    safe_cast(member_party_greece_4 as string) member_party_greece_4,
    safe_cast(member_party_hungary_4 as string) member_party_hungary_4,
    safe_cast(member_party_ireland_3 as string) member_party_ireland_3,
    safe_cast(member_party_israel_3 as string) member_party_israel_3,
    safe_cast(member_party_italy_2 as string) member_party_italy_2,
    safe_cast(member_party_luxembourg as string) member_party_luxembourg,
    safe_cast(member_party_netherlands_3 as string) member_party_netherlands_3,
    safe_cast(member_party_norway_2 as string) member_party_norway_2,
    safe_cast(member_party_poland_5 as string) member_party_poland_5,
    safe_cast(member_party_portugal_3 as string) member_party_portugal_3,
    safe_cast(member_party_sweden_2 as string) member_party_sweden_2,
    safe_cast(member_party_slovenia_4 as string) member_party_slovenia_4,
    safe_cast(left_right_scale as int64) left_right_scale,
    safe_cast(life_satisfaction as int64) life_satisfaction,
    safe_cast(satisfaction_economy as int64) satisfaction_economy,
    safe_cast(satisfaction_government as int64) satisfaction_government,
    safe_cast(satisfaction_democracy as int64) satisfaction_democracy,
    safe_cast(satisfaction_education as int64) satisfaction_education,
    safe_cast(satisfaction_health_services as int64) satisfaction_health_services,
    safe_cast(
        preferred_decision_level_environmental_protection_policies as string
    ) preferred_decision_level_environmental_protection_policies,
    safe_cast(
        preferred_decision_level_fighting_against_organised as string
    ) preferred_decision_level_fighting_against_organised,
    safe_cast(
        preferred_decision_level_agricultural_policies as string
    ) preferred_decision_level_agricultural_policies,
    safe_cast(
        preferred_decision_level_defence_policies as string
    ) preferred_decision_level_defence_policies,
    safe_cast(
        preferred_decision_level_social_welfare_policies as string
    ) preferred_decision_level_social_welfare_policies,
    safe_cast(
        preferred_decision_level_policies_aid_developing as string
    ) preferred_decision_level_policies_aid_developing,
    safe_cast(
        preferred_decision_level_immigration_refugees_policies as string
    ) preferred_decision_level_immigration_refugees_policies,
    safe_cast(
        preferred_decision_level_interest_rates_policies as string
    ) preferred_decision_level_interest_rates_policies,
    safe_cast(
        less_government_intervenes_economy_better_country as string
    ) less_government_intervenes_economy_better_country,
    safe_cast(
        government_reduce_income_differences as string
    ) government_reduce_income_differences,
    safe_cast(
        employees_need_strong_trade_unions_protect as string
    ) employees_need_strong_trade_unions_protect,
    safe_cast(gays_free_to_live_as_wish as string) gays_free_to_live_as_wish,
    safe_cast(law_should_always_obeyed as string) law_should_always_obeyed,
    safe_cast(
        ban_political_parties_wish_overthrow_democracy as string
    ) ban_political_parties_wish_overthrow_democracy,
    safe_cast(
        economic_growth_always_ends_up_harming as string
    ) economic_growth_always_ends_up_harming,
    safe_cast(
        modern_science_can_relied_solve_environmental as string
    ) modern_science_can_relied_solve_environmental,
    safe_cast(happiness as int64) happiness,
    safe_cast(frequency_social_meetings as string) frequency_social_meetings,
    safe_cast(
        anyone_discuss_intimate_personal_matters as string
    ) anyone_discuss_intimate_personal_matters,
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
    safe_cast(citizenship_5 as string) citizenship_5,
    safe_cast(born_in_country as string) born_in_country,
    safe_cast(country_birth_5 as string) country_birth_5,
    safe_cast(
        long_ago_first_came_live_country as string
    ) long_ago_first_came_live_country,
    safe_cast(language_home_first_mentioned as string) language_home_first_mentioned,
    safe_cast(language_home_second_mentioned as string) language_home_second_mentioned,
    safe_cast(belong_ethnic_minority as string) belong_ethnic_minority,
    safe_cast(father_born_in_country as string) father_born_in_country,
    safe_cast(continent_birth_father as string) continent_birth_father,
    safe_cast(mother_born_in_country as string) mother_born_in_country,
    safe_cast(continent_birth_mother as string) continent_birth_mother,
    safe_cast(
        most_immigrants_country_same_race_ethnic as string
    ) most_immigrants_country_same_race_ethnic,
    safe_cast(
        immigrants_from_europe_most_from_rich_poor as string
    ) immigrants_from_europe_most_from_rich_poor,
    safe_cast(
        immigrants_from_outside_from_rich_poor_countries as string
    ) immigrants_from_outside_from_rich_poor_countries,
    safe_cast(
        allow_immigrants_same_ethnicity as string
    ) allow_immigrants_same_ethnicity,
    safe_cast(
        allow_immigrants_different_ethnicity as string
    ) allow_immigrants_different_ethnicity,
    safe_cast(
        allow_many_few_immigrants_from_richer as string
    ) allow_many_few_immigrants_from_richer,
    safe_cast(
        allow_many_few_immigrants_from_poorer as string
    ) allow_many_few_immigrants_from_poorer,
    safe_cast(
        allow_many_few_immigrants_from_richer_2 as string
    ) allow_many_few_immigrants_from_richer_2,
    safe_cast(
        allow_immigrants_poorer_countries as string
    ) allow_immigrants_poorer_countries,
    safe_cast(
        qualification_immigration_good_educational_qualifications as int64
    ) qualification_immigration_good_educational_qualifications,
    safe_cast(
        qualification_immigration_close_family_living_here as int64
    ) qualification_immigration_close_family_living_here,
    safe_cast(
        qualification_immigration_speak_country_official_language as int64
    ) qualification_immigration_speak_country_official_language,
    safe_cast(
        qualification_immigration_christian_background as int64
    ) qualification_immigration_christian_background,
    safe_cast(qualification_immigration_white as int64) qualification_immigration_white,
    safe_cast(
        qualification_immigration_wealthy as int64
    ) qualification_immigration_wealthy,
    safe_cast(
        qualification_immigration_work_skills_needed_country as int64
    ) qualification_immigration_work_skills_needed_country,
    safe_cast(
        qualification_immigration_committed_way_life_country as int64
    ) qualification_immigration_committed_way_life_country,
    safe_cast(
        average_wages_salaries_generally_brought_down as string
    ) average_wages_salaries_generally_brought_down,
    safe_cast(
        immigrants_harm_economic_prospects_poor_more as string
    ) immigrants_harm_economic_prospects_poor_more,
    safe_cast(
        immigrants_help_fill_jobs_where_there as string
    ) immigrants_help_fill_jobs_where_there,
    safe_cast(
        if_immigrants_long_term_unemployed_they as string
    ) if_immigrants_long_term_unemployed_they,
    safe_cast(
        immigrants_should_given_same_rights_everyone as string
    ) immigrants_should_given_same_rights_everyone,
    safe_cast(
        if_immigrants_commit_serious_crime_they as string
    ) if_immigrants_commit_serious_crime_they,
    safe_cast(
        if_immigrants_commit_any_crime_they as string
    ) if_immigrants_commit_any_crime_they,
    safe_cast(
        immigrants_take_jobs_away_country_create as int64
    ) immigrants_take_jobs_away_country_create,
    safe_cast(
        taxes_services_immigrants_take_out_more as int64
    ) taxes_services_immigrants_take_out_more,
    safe_cast(immigration_good_for_economy as int64) immigration_good_for_economy,
    safe_cast(immigration_undermines_culture as int64) immigration_undermines_culture,
    safe_cast(immigrants_make_country_better as int64) immigrants_make_country_better,
    safe_cast(
        immigrants_make_country_crime_problems_worse as int64
    ) immigrants_make_country_crime_problems_worse,
    safe_cast(
        immigration_country_bad_good_home_countries as int64
    ) immigration_country_bad_good_home_countries,
    safe_cast(
        all_countries_benefit_if_people_can as string
    ) all_countries_benefit_if_people_can,
    safe_cast(
        richer_countries_responsible_accept_people_from as string
    ) richer_countries_responsible_accept_people_from,
    safe_cast(immigrant_same_race_boss as int64) immigrant_same_race_boss,
    safe_cast(
        immigrant_same_race_married_close_relative as int64
    ) immigrant_same_race_married_close_relative,
    safe_cast(immigrant_different_race_boss as int64) immigrant_different_race_boss,
    safe_cast(
        immigrant_different_race_married_close_relative as int64
    ) immigrant_different_race_married_close_relative,
    safe_cast(
        people_minority_race_ethnic_group_ideal as string
    ) people_minority_race_ethnic_group_ideal,
    safe_cast(
        people_minority_race_ethnic_group_current as string
    ) people_minority_race_ethnic_group_current,
    safe_cast(
        better_country_if_almost_everyone_shares as string
    ) better_country_if_almost_everyone_shares,
    safe_cast(
        better_country_if_variety_different_religions as string
    ) better_country_if_variety_different_religions,
    safe_cast(
        better_country_if_almost_everyone_speak as string
    ) better_country_if_almost_everyone_speak,
    safe_cast(
        immigrant_communities_should_allowed_separate_schools as string
    ) immigrant_communities_should_allowed_separate_schools,
    safe_cast(
        if_country_wants_reduce_tension_it as string
    ) if_country_wants_reduce_tension_it,
    safe_cast(
        law_against_ethnic_discrimination_workplace_good as int64
    ) law_against_ethnic_discrimination_workplace_good,
    safe_cast(
        law_against_promoting_racial_ethnic_hatred as int64
    ) law_against_promoting_racial_ethnic_hatred,
    safe_cast(any_immigrant_friends as string) any_immigrant_friends,
    safe_cast(any_immigrant_colleagues as string) any_immigrant_colleagues,
    safe_cast(country_has_more_than_its_fair as string) country_has_more_than_its_fair,
    safe_cast(
        people_applying_refugee_status_allowed_work as string
    ) people_applying_refugee_status_allowed_work,
    safe_cast(
        government_should_generous_judging_applications_refugee as string
    ) government_should_generous_judging_applications_refugee,
    safe_cast(
        most_refugee_applicants_not_real_fear as string
    ) most_refugee_applicants_not_real_fear,
    safe_cast(
        refugee_applicants_kept_detention_centres_while as string
    ) refugee_applicants_kept_detention_centres_while,
    safe_cast(
        financial_support_refugee_applicants_while_cases as string
    ) financial_support_refugee_applicants_while_cases,
    safe_cast(
        granted_refugees_should_entitled_bring_close as string
    ) granted_refugees_should_entitled_bring_close,
    safe_cast(
        every_100_people_country_number_born as string
    ) every_100_people_country_number_born,
    safe_cast(
        country_number_immigrants_compared_european_countries as string
    ) country_number_immigrants_compared_european_countries,
    safe_cast(
        number_people_leaving_country_compared_coming as string
    ) number_people_leaving_country_compared_coming,
    safe_cast(
        sports_outdoor_activity_refusal as string
    ) sports_outdoor_activity_refusal,
    safe_cast(
        sports_outdoor_activity_no_answer as string
    ) sports_outdoor_activity_no_answer,
    safe_cast(
        sports_outdoor_activity_none_apply as string
    ) sports_outdoor_activity_none_apply,
    safe_cast(sports_outdoor_activity_member as string) sports_outdoor_activity_member,
    safe_cast(
        sports_outdoor_activity_participated as string
    ) sports_outdoor_activity_participated,
    safe_cast(
        sports_outdoor_activity_donated_money as string
    ) sports_outdoor_activity_donated_money,
    safe_cast(
        sports_outdoor_activity_voluntary_work as string
    ) sports_outdoor_activity_voluntary_work,
    safe_cast(
        personal_friends_sports_outdoor_activity_club as string
    ) personal_friends_sports_outdoor_activity_club,
    safe_cast(
        personal_friends_cultural_hobby_activity_organisation as string
    ) personal_friends_cultural_hobby_activity_organisation,
    safe_cast(personal_friends_trade_union as string) personal_friends_trade_union,
    safe_cast(
        personal_friends_business_profession_farmers_organisation as string
    ) personal_friends_business_profession_farmers_organisation,
    safe_cast(
        personal_friends_consumer_automobile_organisation as string
    ) personal_friends_consumer_automobile_organisation,
    safe_cast(
        personal_friends_humanitarian_organisation_etc as string
    ) personal_friends_humanitarian_organisation_etc,
    safe_cast(
        personal_friends_environmental_peace_animal_organisation as string
    ) personal_friends_environmental_peace_animal_organisation,
    safe_cast(
        personal_friends_religious_church_organisation as string
    ) personal_friends_religious_church_organisation,
    safe_cast(
        personal_friends_political_party as string
    ) personal_friends_political_party,
    safe_cast(
        personal_friends_science_education_teacher_organisation as string
    ) personal_friends_science_education_teacher_organisation,
    safe_cast(
        personal_friends_social_club_etc as string
    ) personal_friends_social_club_etc,
    safe_cast(
        personal_friends_other_voluntary_organisation as string
    ) personal_friends_other_voluntary_organisation,
    safe_cast(
        cultural_hobby_activity_refusal as string
    ) cultural_hobby_activity_refusal,
    safe_cast(
        cultural_hobby_activity_no_answer as string
    ) cultural_hobby_activity_no_answer,
    safe_cast(
        cultural_hobby_activity_none_apply as string
    ) cultural_hobby_activity_none_apply,
    safe_cast(cultural_hobby_activity_member as string) cultural_hobby_activity_member,
    safe_cast(
        cultural_hobby_activity_participated as string
    ) cultural_hobby_activity_participated,
    safe_cast(
        cultural_hobby_activity_donated_money as string
    ) cultural_hobby_activity_donated_money,
    safe_cast(
        cultural_hobby_activity_voluntary_work as string
    ) cultural_hobby_activity_voluntary_work,
    safe_cast(trade_union_12_refusal as string) trade_union_12_refusal,
    safe_cast(trade_union_12_no_answer as string) trade_union_12_no_answer,
    safe_cast(trade_union_12_none_apply as string) trade_union_12_none_apply,
    safe_cast(trade_union_12_member as string) trade_union_12_member,
    safe_cast(trade_union_12_participated as string) trade_union_12_participated,
    safe_cast(trade_union_12_donated_money as string) trade_union_12_donated_money,
    safe_cast(trade_union_12_voluntary_work as string) trade_union_12_voluntary_work,
    safe_cast(
        business_profession_farmers_refusal as string
    ) business_profession_farmers_refusal,
    safe_cast(
        business_profession_farmers_no_answer as string
    ) business_profession_farmers_no_answer,
    safe_cast(
        business_profession_farmers_none_apply as string
    ) business_profession_farmers_none_apply,
    safe_cast(
        business_profession_farmers_member as string
    ) business_profession_farmers_member,
    safe_cast(
        business_profession_farmers_participated as string
    ) business_profession_farmers_participated,
    safe_cast(
        business_profession_farmer_donated_money as string
    ) business_profession_farmer_donated_money,
    safe_cast(
        business_profession_farmer_voluntary_work as string
    ) business_profession_farmer_voluntary_work,
    safe_cast(
        consumer_automobile_organisation_refusal as string
    ) consumer_automobile_organisation_refusal,
    safe_cast(
        consumer_automobile_organisation_no_answer as string
    ) consumer_automobile_organisation_no_answer,
    safe_cast(
        consumer_automobile_organisation_none_apply as string
    ) consumer_automobile_organisation_none_apply,
    safe_cast(
        consumer_automobile_organisation_member as string
    ) consumer_automobile_organisation_member,
    safe_cast(
        consumer_automobile_organisation_participated as string
    ) consumer_automobile_organisation_participated,
    safe_cast(
        consumer_automobile_organisation_donated_money as string
    ) consumer_automobile_organisation_donated_money,
    safe_cast(
        consumer_automobile_organisation_voluntary_work as string
    ) consumer_automobile_organisation_voluntary_work,
    safe_cast(
        humanitarian_organisation_etc_refusal as string
    ) humanitarian_organisation_etc_refusal,
    safe_cast(
        humanitarian_organisation_etc_no_answer as string
    ) humanitarian_organisation_etc_no_answer,
    safe_cast(
        humanitarian_organisation_etc_none_apply as string
    ) humanitarian_organisation_etc_none_apply,
    safe_cast(
        humanitarian_organisation_etc_member as string
    ) humanitarian_organisation_etc_member,
    safe_cast(
        humanitarian_organisation_etc_participated as string
    ) humanitarian_organisation_etc_participated,
    safe_cast(
        humanitarian_organisation_etc_donated_money as string
    ) humanitarian_organisation_etc_donated_money,
    safe_cast(
        humanitarian_organisation_etc_voluntary_work as string
    ) humanitarian_organisation_etc_voluntary_work,
    safe_cast(
        environmental_peace_animal_refusal as string
    ) environmental_peace_animal_refusal,
    safe_cast(
        environment_peace_animal_no_answer as string
    ) environment_peace_animal_no_answer,
    safe_cast(
        environmental_peace_animal_none_apply as string
    ) environmental_peace_animal_none_apply,
    safe_cast(
        environmental_peace_animal_member as string
    ) environmental_peace_animal_member,
    safe_cast(
        environmental_peace_animal_participated as string
    ) environmental_peace_animal_participated,
    safe_cast(
        environmental_peace_animal_donated_money as string
    ) environmental_peace_animal_donated_money,
    safe_cast(
        environment_peace_animal_voluntary_work as string
    ) environment_peace_animal_voluntary_work,
    safe_cast(
        religious_church_organisation_refusal as string
    ) religious_church_organisation_refusal,
    safe_cast(
        religious_church_organisation_no_answer as string
    ) religious_church_organisation_no_answer,
    safe_cast(
        religious_church_organisation_none_apply as string
    ) religious_church_organisation_none_apply,
    safe_cast(
        religious_church_organisation_member as string
    ) religious_church_organisation_member,
    safe_cast(
        religious_church_organisation_participated as string
    ) religious_church_organisation_participated,
    safe_cast(
        religious_church_organisation_donated_money as string
    ) religious_church_organisation_donated_money,
    safe_cast(
        religious_church_organisation_voluntary_work as string
    ) religious_church_organisation_voluntary_work,
    safe_cast(political_party_12_refusal as string) political_party_12_refusal,
    safe_cast(political_party_12_no_answer as string) political_party_12_no_answer,
    safe_cast(political_party_12_none_apply as string) political_party_12_none_apply,
    safe_cast(political_party_12_member as string) political_party_12_member,
    safe_cast(
        political_party_12_participated as string
    ) political_party_12_participated,
    safe_cast(
        political_party_12_donated_money as string
    ) political_party_12_donated_money,
    safe_cast(
        political_party_12_voluntary_work as string
    ) political_party_12_voluntary_work,
    safe_cast(
        science_education_teacher_refusal as string
    ) science_education_teacher_refusal,
    safe_cast(
        science_education_teacher_no_answer as string
    ) science_education_teacher_no_answer,
    safe_cast(
        science_education_teacher_none_apply as string
    ) science_education_teacher_none_apply,
    safe_cast(
        science_education_teacher_member as string
    ) science_education_teacher_member,
    safe_cast(
        science_education_teacher_participated as string
    ) science_education_teacher_participated,
    safe_cast(
        science_education_teacher_donated_money as string
    ) science_education_teacher_donated_money,
    safe_cast(
        science_education_teacher_voluntary_work as string
    ) science_education_teacher_voluntary_work,
    safe_cast(social_club_etc_refusal as string) social_club_etc_refusal,
    safe_cast(social_club_etc_no_answer as string) social_club_etc_no_answer,
    safe_cast(social_club_etc_none_apply as string) social_club_etc_none_apply,
    safe_cast(social_club_etc_member as string) social_club_etc_member,
    safe_cast(social_club_etc_participated as string) social_club_etc_participated,
    safe_cast(social_club_etc_donated_money as string) social_club_etc_donated_money,
    safe_cast(social_club_etc_voluntary_work as string) social_club_etc_voluntary_work,
    safe_cast(
        other_voluntary_organisation_refusal as string
    ) other_voluntary_organisation_refusal,
    safe_cast(
        other_voluntary_organisation_no_answer as string
    ) other_voluntary_organisation_no_answer,
    safe_cast(
        other_voluntary_organisation_none_apply as string
    ) other_voluntary_organisation_none_apply,
    safe_cast(
        other_voluntary_organisation_member as string
    ) other_voluntary_organisation_member,
    safe_cast(
        other_voluntary_organisation_participated as string
    ) other_voluntary_organisation_participated,
    safe_cast(
        other_voluntary_organisation_donated_money as string
    ) other_voluntary_organisation_donated_money,
    safe_cast(
        other_voluntary_organisation_voluntary_work as string
    ) other_voluntary_organisation_voluntary_work,
    safe_cast(important_life_family as int64) important_life_family,
    safe_cast(important_life_friends as int64) important_life_friends,
    safe_cast(important_life_leisure_time as int64) important_life_leisure_time,
    safe_cast(important_life_politics as int64) important_life_politics,
    safe_cast(important_life_work as int64) important_life_work,
    safe_cast(important_life_religion as int64) important_life_religion,
    safe_cast(
        important_life_voluntary_organisations as int64
    ) important_life_voluntary_organisations,
    safe_cast(
        help_others_not_counting_work_voluntary as string
    ) help_others_not_counting_work_voluntary,
    safe_cast(
        discuss_politics_current_affairs_frequency as string
    ) discuss_politics_current_affairs_frequency,
    safe_cast(
        good_citizen_important_support_people_worse as int64
    ) good_citizen_important_support_people_worse,
    safe_cast(
        good_citizen_important_vote_elections as int64
    ) good_citizen_important_vote_elections,
    safe_cast(
        good_citizen_important_always_obey_laws as int64
    ) good_citizen_important_always_obey_laws,
    safe_cast(
        good_citizen_important_form_independent_opinion as int64
    ) good_citizen_important_form_independent_opinion,
    safe_cast(
        good_citizen_important_active_voluntary_organisations as int64
    ) good_citizen_important_active_voluntary_organisations,
    safe_cast(
        good_citizen_important_active_politics as int64
    ) good_citizen_important_active_politics,
    safe_cast(long_lived_area as string) long_lived_area,
    safe_cast(employment_status as string) employment_status,
    safe_cast(allowed_flexible_working_hours as int64) allowed_flexible_working_hours,
    safe_cast(
        allowed_decide_daily_work_organised_2 as int64
    ) allowed_decide_daily_work_organised_2,
    safe_cast(
        allowed_influence_job_environment as int64
    ) allowed_influence_job_environment,
    safe_cast(
        allowed_influence_decisions_work_direction as int64
    ) allowed_influence_decisions_work_direction,
    safe_cast(allowed_change_work_tasks as int64) allowed_change_work_tasks,
    safe_cast(
        get_similar_better_job_another_employer_2 as int64
    ) get_similar_better_job_another_employer_2,
    safe_cast(start_own_business as int64) start_own_business,
    safe_cast(trade_union_workplace as string) trade_union_workplace,
    safe_cast(
        difficult_easy_have_say_actions_taken as int64
    ) difficult_easy_have_say_actions_taken,
    safe_cast(
        difficult_easy_trade_union_influence_conditions as int64
    ) difficult_easy_trade_union_influence_conditions,
    safe_cast(
        satisfaction_way_things_handled_workplace_12 as int64
    ) satisfaction_way_things_handled_workplace_12,
    safe_cast(
        attempted_improve_work_conditions_12_months as string
    ) attempted_improve_work_conditions_12_months,
    safe_cast(
        did_any_improvement_work_conditions_result as string
    ) did_any_improvement_work_conditions_result,
    safe_cast(
        fairly_unfairly_treated_attempt_improve_things as int64
    ) fairly_unfairly_treated_attempt_improve_things,
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
        second_person_household_relationship_respondent as string
    ) second_person_household_relationship_respondent,
    safe_cast(
        third_person_household_relationship_respondent as string
    ) third_person_household_relationship_respondent,
    safe_cast(
        fourth_person_household_relationship_respondent as string
    ) fourth_person_household_relationship_respondent,
    safe_cast(
        fifth_person_household_relationship_respondent as string
    ) fifth_person_household_relationship_respondent,
    safe_cast(
        sixth_person_household_relationship_respondent as string
    ) sixth_person_household_relationship_respondent,
    safe_cast(
        seventh_person_household_relationship_respondent as string
    ) seventh_person_household_relationship_respondent,
    safe_cast(
        eighth_person_household_relationship_respondent as string
    ) eighth_person_household_relationship_respondent,
    safe_cast(
        ninth_person_household_relationship_respondent as string
    ) ninth_person_household_relationship_respondent,
    safe_cast(
        tenth_person_household_relationship_respondent as string
    ) tenth_person_household_relationship_respondent,
    safe_cast(
        eleventh_person_household_relationship_respondent as string
    ) eleventh_person_household_relationship_respondent,
    safe_cast(
        twelfth_person_household_relationship_respondent as string
    ) twelfth_person_household_relationship_respondent,
    safe_cast(
        thirteenth_person_household_relationship_respondent as string
    ) thirteenth_person_household_relationship_respondent,
    safe_cast(
        fourteenth_person_household_relationship_respondent as string
    ) fourteenth_person_household_relationship_respondent,
    safe_cast(
        fifteenth_person_household_relationship_respondent as string
    ) fifteenth_person_household_relationship_respondent,
    safe_cast(domicile_type as string) domicile_type,
    safe_cast(highest_education_3 as string) highest_education_3,
    safe_cast(highest_education_es_isced as string) highest_education_es_isced,
    safe_cast(highest_education_belgium_4 as string) highest_education_belgium_4,
    safe_cast(
        highest_education_switzerland_5 as string
    ) highest_education_switzerland_5,
    safe_cast(highest_education_czechia_2 as string) highest_education_czechia_2,
    safe_cast(highest_education_denmark_3 as string) highest_education_denmark_3,
    safe_cast(highest_education_spain_6 as string) highest_education_spain_6,
    safe_cast(highest_education_france_4 as string) highest_education_france_4,
    safe_cast(
        highest_education_united_kingdom_8 as string
    ) highest_education_united_kingdom_8,
    safe_cast(highest_education_greece_4 as string) highest_education_greece_4,
    safe_cast(highest_education_hungary_5 as string) highest_education_hungary_5,
    safe_cast(highest_education_ireland_4 as string) highest_education_ireland_4,
    safe_cast(highest_education_israel_2 as string) highest_education_israel_2,
    safe_cast(highest_education_italy_5 as string) highest_education_italy_5,
    safe_cast(highest_education_luxembourg as string) highest_education_luxembourg,
    safe_cast(
        highest_education_netherlands_3 as string
    ) highest_education_netherlands_3,
    safe_cast(highest_education_norway_4 as string) highest_education_norway_4,
    safe_cast(highest_education_poland_7 as string) highest_education_poland_7,
    safe_cast(highest_education_portugal_5 as string) highest_education_portugal_5,
    safe_cast(highest_education_sweden_3 as string) highest_education_sweden_3,
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
    safe_cast(
        employment_contract_unlimited_limited_duration as string
    ) employment_contract_unlimited_limited_duration,
    safe_cast(
        employment_contract_unlimited_limited_duration_hungary as string
    ) employment_contract_unlimited_limited_duration_hungary,
    safe_cast(establishment_size as string) establishment_size,
    safe_cast(
        responsible_supervising_other_employees as string
    ) responsible_supervising_other_employees,
    safe_cast(number_people_responsible_job as int64) number_people_responsible_job,
    safe_cast(what_extent_organise_own_work as string) what_extent_organise_own_work,
    safe_cast(contracted_weekly_hours as int64) contracted_weekly_hours,
    safe_cast(total_weekly_hours as int64) total_weekly_hours,
    safe_cast(occupation_isco88_com as string) occupation_isco88_com,
    safe_cast(industry_nace_rev_1 as string) industry_nace_rev_1,
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
    safe_cast(main_source_household_income_2 as string) main_source_household_income_2,
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
        father_occupation_when_respondent_14_3 as string
    ) father_occupation_when_respondent_14_3,
    safe_cast(
        father_occupation_when_respondent_14_ireland as string
    ) father_occupation_when_respondent_14_ireland,
    safe_cast(mother_highest_education_3 as string) mother_highest_education_3,
    safe_cast(
        mother_employment_status_when_respondent_14 as string
    ) mother_employment_status_when_respondent_14,
    safe_cast(number_employees_mother_had as string) number_employees_mother_had,
    safe_cast(
        mother_responsible_supervising_other_employees as string
    ) mother_responsible_supervising_other_employees,
    safe_cast(
        mother_occupation_when_respondent_14_3 as string
    ) mother_occupation_when_respondent_14_3,
    safe_cast(
        mother_occupation_when_respondent_14_ireland as string
    ) mother_occupation_when_respondent_14_ireland,
    safe_cast(
        improve_skills_course_lecture_conference_12 as string
    ) improve_skills_course_lecture_conference_12,
    safe_cast(legal_marital_status_4 as string) legal_marital_status_4,
    safe_cast(legal_marital_status_france as string) legal_marital_status_france,
    safe_cast(currently_living_husband_wife as string) currently_living_husband_wife,
    safe_cast(
        currently_living_another_partner_than_husband as string
    ) currently_living_another_partner_than_husband,
    safe_cast(currently_living_partner_2 as string) currently_living_partner_2,
    safe_cast(
        ever_lived_partner_without_married_2 as string
    ) ever_lived_partner_without_married_2,
    safe_cast(ever_divorced as string) ever_divorced,
    safe_cast(children_living_home_not as string) children_living_home_not,
    safe_cast(children_living_at_home as string) children_living_at_home,
    safe_cast(region_austria as string) region_austria,
    safe_cast(region_belgium as string) region_belgium,
    safe_cast(region_switzerland as string) region_switzerland,
    safe_cast(region_czechia_2 as string) region_czechia_2,
    safe_cast(region_germany as string) region_germany,
    safe_cast(region_denmark_2 as string) region_denmark_2,
    safe_cast(region_spain_2 as string) region_spain_2,
    safe_cast(region_finland_2 as string) region_finland_2,
    safe_cast(region_france as string) region_france,
    safe_cast(region_united_kingdom as string) region_united_kingdom,
    safe_cast(region_greece_2 as string) region_greece_2,
    safe_cast(region_hungary as string) region_hungary,
    safe_cast(region_ireland_3 as string) region_ireland_3,
    safe_cast(region_israel as string) region_israel,
    safe_cast(region_italy as string) region_italy,
    safe_cast(region_luxembourg as string) region_luxembourg,
    safe_cast(region_netherlands as string) region_netherlands,
    safe_cast(region_norway as string) region_norway,
    safe_cast(region_poland as string) region_poland,
    safe_cast(region_portugal_2 as string) region_portugal_2,
    safe_cast(region_sweden as string) region_sweden,
    safe_cast(region_slovenia as string) region_slovenia,
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
    safe_cast(day_month_interview as string) day_month_interview,
    safe_cast(month_interview as string) month_interview,
    safe_cast(year_interview as string) year_interview,
    safe_cast(start_interview_hour as string) start_interview_hour,
    safe_cast(start_interview_minute as string) start_interview_minute,
    safe_cast(end_interview_minute as string) end_interview_minute,
    safe_cast(end_interview_hour as string) end_interview_hour,
    safe_cast(
        interview_length_minutes_main_questionnaire as int64
    ) interview_length_minutes_main_questionnaire,
    safe_cast(
        administration_split_ballot_mtmm_7 as string
    ) administration_split_ballot_mtmm_7,
    safe_cast(
        administration_supplementary_questionnaire as string
    ) administration_supplementary_questionnaire
from {{ set_datalake_project("gb_eric_ess_staging.round_01") }} as t
