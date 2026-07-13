{{
    config(
        schema="gb_eric_ess",
        alias="round_09",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2018, "end": 2023, "interval": 1},
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
    safe_cast(party_voted_austria_2 as string) party_voted_austria_2,
    safe_cast(party_voted_belgium_2 as string) party_voted_belgium_2,
    safe_cast(party_voted_bulgaria_7 as string) party_voted_bulgaria_7,
    safe_cast(party_voted_switzerland_2 as string) party_voted_switzerland_2,
    safe_cast(party_voted_cyprus_2 as string) party_voted_cyprus_2,
    safe_cast(party_voted_czechia as string) party_voted_czechia,
    safe_cast(party_voted_1_germany_2 as string) party_voted_1_germany_2,
    safe_cast(party_voted_2_germany_2 as string) party_voted_2_germany_2,
    safe_cast(party_voted_denmark as string) party_voted_denmark,
    safe_cast(party_voted_estonia_2 as string) party_voted_estonia_2,
    safe_cast(party_voted_spain_2 as string) party_voted_spain_2,
    safe_cast(party_voted_finland_3 as string) party_voted_finland_3,
    safe_cast(party_voted_france_ballot_1_3 as string) party_voted_france_ballot_1_3,
    safe_cast(party_voted_united_kingdom_2 as string) party_voted_united_kingdom_2,
    safe_cast(party_voted_croatia_3 as string) party_voted_croatia_3,
    safe_cast(party_voted_hungary_3 as string) party_voted_hungary_3,
    safe_cast(
        party_voted_ireland_derived_from_1st_3 as string
    ) party_voted_ireland_derived_from_1st_3,
    safe_cast(party_voted_iceland_3 as string) party_voted_iceland_3,
    safe_cast(party_voted_italy_6 as string) party_voted_italy_6,
    safe_cast(
        party_voted_1_lithuania_first_vote_party_2 as string
    ) party_voted_1_lithuania_first_vote_party_2,
    safe_cast(
        party_voted_2_lithuania_second_vote_party_2 as string
    ) party_voted_2_lithuania_second_vote_party_2,
    safe_cast(
        party_voted_3_lithuania_third_vote_party_2 as string
    ) party_voted_3_lithuania_third_vote_party_2,
    safe_cast(party_voted_latvia_2 as string) party_voted_latvia_2,
    safe_cast(party_voted_montenegro_3 as string) party_voted_montenegro_3,
    safe_cast(party_voted_netherlands_3 as string) party_voted_netherlands_3,
    safe_cast(party_voted_norway_2 as string) party_voted_norway_2,
    safe_cast(party_voted_poland_2 as string) party_voted_poland_2,
    safe_cast(party_voted_portugal_3 as string) party_voted_portugal_3,
    safe_cast(party_voted_serbia_2 as string) party_voted_serbia_2,
    safe_cast(party_voted_sweden_2 as string) party_voted_sweden_2,
    safe_cast(party_voted_slovenia_2 as string) party_voted_slovenia_2,
    safe_cast(party_voted_slovakia_2 as string) party_voted_slovakia_2,
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
    safe_cast(posted_shared_politics_online as string) posted_shared_politics_online,
    safe_cast(feel_close_to_party as string) feel_close_to_party,
    safe_cast(party_feel_close_austria_2 as string) party_feel_close_austria_2,
    safe_cast(party_feel_close_belgium_2 as string) party_feel_close_belgium_2,
    safe_cast(party_feel_close_bulgaria_3 as string) party_feel_close_bulgaria_3,
    safe_cast(party_feel_close_switzerland_2 as string) party_feel_close_switzerland_2,
    safe_cast(party_feel_close_cyprus_2 as string) party_feel_close_cyprus_2,
    safe_cast(party_feel_close_czechia as string) party_feel_close_czechia,
    safe_cast(party_feel_close_germany_2 as string) party_feel_close_germany_2,
    safe_cast(party_feel_close_denmark as string) party_feel_close_denmark,
    safe_cast(party_feel_close_estonia_2 as string) party_feel_close_estonia_2,
    safe_cast(party_feel_close_spain_2 as string) party_feel_close_spain_2,
    safe_cast(party_feel_close_finland_3 as string) party_feel_close_finland_3,
    safe_cast(party_feel_close_france_2 as string) party_feel_close_france_2,
    safe_cast(
        party_feel_close_united_kingdom_2 as string
    ) party_feel_close_united_kingdom_2,
    safe_cast(party_feel_close_croatia_2 as string) party_feel_close_croatia_2,
    safe_cast(party_feel_close_hungary_3 as string) party_feel_close_hungary_3,
    safe_cast(party_feel_close_ireland_2 as string) party_feel_close_ireland_2,
    safe_cast(party_feel_close_iceland_3 as string) party_feel_close_iceland_3,
    safe_cast(party_feel_close_italy_3 as string) party_feel_close_italy_3,
    safe_cast(party_feel_close_lithuania_2 as string) party_feel_close_lithuania_2,
    safe_cast(party_feel_close_latvia_2 as string) party_feel_close_latvia_2,
    safe_cast(party_feel_close_montenegro_3 as string) party_feel_close_montenegro_3,
    safe_cast(party_feel_close_netherlands_3 as string) party_feel_close_netherlands_3,
    safe_cast(party_feel_close_norway_2 as string) party_feel_close_norway_2,
    safe_cast(party_feel_close_poland_2 as string) party_feel_close_poland_2,
    safe_cast(party_feel_close_portugal_3 as string) party_feel_close_portugal_3,
    safe_cast(party_feel_close_serbia_2 as string) party_feel_close_serbia_2,
    safe_cast(party_feel_close_sweden_2 as string) party_feel_close_sweden_2,
    safe_cast(party_feel_close_slovenia_2 as string) party_feel_close_slovenia_2,
    safe_cast(party_feel_close_slovakia_2 as string) party_feel_close_slovakia_2,
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
        religion_denomination_belonging_present_belgium as string
    ) religion_denomination_belonging_present_belgium,
    safe_cast(
        religion_denomination_belonging_present_switzerland as string
    ) religion_denomination_belonging_present_switzerland,
    safe_cast(
        religion_denomination_belonging_present_cyprus_2 as string
    ) religion_denomination_belonging_present_cyprus_2,
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
        religion_denomination_belonging_present_iceland as string
    ) religion_denomination_belonging_present_iceland,
    safe_cast(
        religion_denomination_belonging_present_lithuania as string
    ) religion_denomination_belonging_present_lithuania,
    safe_cast(
        religion_denomination_belonging_present_latvia as string
    ) religion_denomination_belonging_present_latvia,
    safe_cast(
        religion_denomination_belonging_present_montenegro as string
    ) religion_denomination_belonging_present_montenegro,
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
        religion_denomination_belonging_present_serbia as string
    ) religion_denomination_belonging_present_serbia,
    safe_cast(
        religion_denomination_belonging_present_sweden as string
    ) religion_denomination_belonging_present_sweden,
    safe_cast(
        religion_denomination_belonging_present_slovakia as string
    ) religion_denomination_belonging_present_slovakia,
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
        religion_denomination_belonging_past_cyprus_2 as string
    ) religion_denomination_belonging_past_cyprus_2,
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
        religion_denomination_belonging_past_iceland as string
    ) religion_denomination_belonging_past_iceland,
    safe_cast(
        religion_denomination_belonging_past_lithuania as string
    ) religion_denomination_belonging_past_lithuania,
    safe_cast(
        religion_denomination_belonging_past_latvia as string
    ) religion_denomination_belonging_past_latvia,
    safe_cast(
        religion_denomination_belonging_past_montenegro as string
    ) religion_denomination_belonging_past_montenegro,
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
        religion_denomination_belonging_past_serbia as string
    ) religion_denomination_belonging_past_serbia,
    safe_cast(
        religion_denomination_belonging_past_sweden as string
    ) religion_denomination_belonging_past_sweden,
    safe_cast(
        religion_denomination_belonging_past_slovakia as string
    ) religion_denomination_belonging_past_slovakia,
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
    safe_cast(citizenship as string) citizenship,
    safe_cast(born_in_country as string) born_in_country,
    safe_cast(country_birth as string) country_birth,
    safe_cast(year_arrived_country as int64) year_arrived_country,
    safe_cast(language_home_first as string) language_home_first,
    safe_cast(language_home_second as string) language_home_second,
    safe_cast(belong_ethnic_minority as string) belong_ethnic_minority,
    safe_cast(father_born_in_country as string) father_born_in_country,
    safe_cast(country_birth_father as string) country_birth_father,
    safe_cast(mother_born_in_country as string) mother_born_in_country,
    safe_cast(country_birth_mother as string) country_birth_mother,
    safe_cast(
        administration_democracy_works_questions as string
    ) administration_democracy_works_questions,
    safe_cast(would_vote as string) would_vote,
    safe_cast(would_vote_3 as string) would_vote_3,
    safe_cast(would_vote_2 as string) would_vote_2,
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
    safe_cast(
        administration_split_ballot_ask_female_male as string
    ) administration_split_ballot_ask_female_male,
    safe_cast(age_become_adults_split_ballot as string) age_become_adults_split_ballot,
    safe_cast(
        age_reach_middle_age_split_ballot as string
    ) age_reach_middle_age_split_ballot,
    safe_cast(age_reach_old_age_split_ballot as string) age_reach_old_age_split_ballot,
    safe_cast(
        start_living_partner_not_married_ideal as string
    ) start_living_partner_not_married_ideal,
    safe_cast(
        get_married_ideal_age_split_ballot as string
    ) get_married_ideal_age_split_ballot,
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
        start_living_partner_not_married_age as string
    ) start_living_partner_not_married_age,
    safe_cast(
        get_married_age_too_young_split_ballot as string
    ) get_married_age_too_young_split_ballot,
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
        plan_future_take_each_day_it_comes as int64
    ) plan_future_take_each_day_it_comes,
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
    safe_cast(highest_education_es_isced as string) highest_education_es_isced,
    safe_cast(highest_education as string) highest_education,
    safe_cast(highest_education_austria as string) highest_education_austria,
    safe_cast(highest_education_belgium as string) highest_education_belgium,
    safe_cast(highest_education_bulgaria as string) highest_education_bulgaria,
    safe_cast(highest_education_switzerland as string) highest_education_switzerland,
    safe_cast(highest_education_cyprus as string) highest_education_cyprus,
    safe_cast(highest_education_czechia as string) highest_education_czechia,
    safe_cast(highest_education_germany_3 as string) highest_education_germany_3,
    safe_cast(highest_education_germany_4 as string) highest_education_germany_4,
    safe_cast(highest_education_germany_5 as string) highest_education_germany_5,
    safe_cast(highest_education_denmark as string) highest_education_denmark,
    safe_cast(highest_education_estonia as string) highest_education_estonia,
    safe_cast(highest_education_spain_2 as string) highest_education_spain_2,
    safe_cast(highest_education_finland as string) highest_education_finland,
    safe_cast(highest_education_france as string) highest_education_france,
    safe_cast(
        highest_education_united_kingdom as string
    ) highest_education_united_kingdom,
    safe_cast(
        highest_education_united_kingdom_2 as string
    ) highest_education_united_kingdom_2,
    safe_cast(
        age_when_completed_full_time_education as int64
    ) age_when_completed_full_time_education,
    safe_cast(highest_education_croatia as string) highest_education_croatia,
    safe_cast(highest_education_hungary_2 as string) highest_education_hungary_2,
    safe_cast(highest_education_ireland as string) highest_education_ireland,
    safe_cast(highest_education_iceland as string) highest_education_iceland,
    safe_cast(highest_education_italy_2 as string) highest_education_italy_2,
    safe_cast(highest_education_lithuania as string) highest_education_lithuania,
    safe_cast(highest_education_latvia_2 as string) highest_education_latvia_2,
    safe_cast(highest_education_montenegro_2 as string) highest_education_montenegro_2,
    safe_cast(highest_education_netherlands as string) highest_education_netherlands,
    safe_cast(highest_education_norway_2 as string) highest_education_norway_2,
    safe_cast(highest_education_poland_2 as string) highest_education_poland_2,
    safe_cast(highest_education_portugal_2 as string) highest_education_portugal_2,
    safe_cast(highest_education_serbia as string) highest_education_serbia,
    safe_cast(highest_education_sweden as string) highest_education_sweden,
    safe_cast(highest_education_slovenia as string) highest_education_slovenia,
    safe_cast(highest_education_slovakia as string) highest_education_slovakia,
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
    safe_cast(respondent_main_source_income as string) respondent_main_source_income,
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
        partner_highest_education_bulgaria as string
    ) partner_highest_education_bulgaria,
    safe_cast(
        partner_highest_education_switzerland as string
    ) partner_highest_education_switzerland,
    safe_cast(
        partner_highest_education_cyprus as string
    ) partner_highest_education_cyprus,
    safe_cast(
        partner_highest_education_czechia as string
    ) partner_highest_education_czechia,
    safe_cast(
        partner_highest_education_germany_3 as string
    ) partner_highest_education_germany_3,
    safe_cast(
        partner_highest_education_germany_4 as string
    ) partner_highest_education_germany_4,
    safe_cast(
        partner_highest_education_germany_5 as string
    ) partner_highest_education_germany_5,
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
        partner_highest_education_united_kingdom as string
    ) partner_highest_education_united_kingdom,
    safe_cast(
        partner_highest_education_united_kingdom_2 as string
    ) partner_highest_education_united_kingdom_2,
    safe_cast(
        partner_age_when_completed_full_time as int64
    ) partner_age_when_completed_full_time,
    safe_cast(
        partner_highest_education_croatia as string
    ) partner_highest_education_croatia,
    safe_cast(
        partner_highest_education_hungary_2 as string
    ) partner_highest_education_hungary_2,
    safe_cast(
        partner_highest_education_ireland as string
    ) partner_highest_education_ireland,
    safe_cast(
        partner_highest_education_iceland as string
    ) partner_highest_education_iceland,
    safe_cast(
        partner_highest_education_italy_2 as string
    ) partner_highest_education_italy_2,
    safe_cast(
        partner_highest_education_lithuania as string
    ) partner_highest_education_lithuania,
    safe_cast(
        partner_highest_education_latvia_2 as string
    ) partner_highest_education_latvia_2,
    safe_cast(
        partner_highest_education_montenegro_2 as string
    ) partner_highest_education_montenegro_2,
    safe_cast(
        partner_highest_education_netherlands as string
    ) partner_highest_education_netherlands,
    safe_cast(
        partner_highest_education_norway_2 as string
    ) partner_highest_education_norway_2,
    safe_cast(
        partner_highest_education_poland_2 as string
    ) partner_highest_education_poland_2,
    safe_cast(
        partner_highest_education_portugal_2 as string
    ) partner_highest_education_portugal_2,
    safe_cast(
        partner_highest_education_serbia as string
    ) partner_highest_education_serbia,
    safe_cast(
        partner_highest_education_sweden as string
    ) partner_highest_education_sweden,
    safe_cast(
        partner_highest_education_slovenia as string
    ) partner_highest_education_slovenia,
    safe_cast(
        partner_highest_education_slovakia as string
    ) partner_highest_education_slovakia,
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
        father_highest_education_bulgaria as string
    ) father_highest_education_bulgaria,
    safe_cast(
        father_highest_education_switzerland as string
    ) father_highest_education_switzerland,
    safe_cast(
        father_highest_education_cyprus as string
    ) father_highest_education_cyprus,
    safe_cast(
        father_highest_education_czechia as string
    ) father_highest_education_czechia,
    safe_cast(
        father_highest_education_germany_3 as string
    ) father_highest_education_germany_3,
    safe_cast(
        father_highest_education_germany_4 as string
    ) father_highest_education_germany_4,
    safe_cast(
        father_highest_education_germany_5 as string
    ) father_highest_education_germany_5,
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
        father_highest_education_united_kingdom as string
    ) father_highest_education_united_kingdom,
    safe_cast(
        father_highest_education_united_kingdom_2 as string
    ) father_highest_education_united_kingdom_2,
    safe_cast(
        father_age_when_completed_full_time as int64
    ) father_age_when_completed_full_time,
    safe_cast(
        father_highest_education_croatia as string
    ) father_highest_education_croatia,
    safe_cast(
        father_highest_education_hungary_2 as string
    ) father_highest_education_hungary_2,
    safe_cast(
        father_highest_education_ireland as string
    ) father_highest_education_ireland,
    safe_cast(
        father_highest_education_iceland as string
    ) father_highest_education_iceland,
    safe_cast(
        father_highest_education_italy_2 as string
    ) father_highest_education_italy_2,
    safe_cast(
        father_highest_education_lithuania as string
    ) father_highest_education_lithuania,
    safe_cast(
        father_highest_education_latvia_2 as string
    ) father_highest_education_latvia_2,
    safe_cast(
        father_highest_education_montenegro_2 as string
    ) father_highest_education_montenegro_2,
    safe_cast(
        father_highest_education_netherlands as string
    ) father_highest_education_netherlands,
    safe_cast(
        father_highest_education_norway_2 as string
    ) father_highest_education_norway_2,
    safe_cast(
        father_highest_education_poland_2 as string
    ) father_highest_education_poland_2,
    safe_cast(
        father_highest_education_portugal_2 as string
    ) father_highest_education_portugal_2,
    safe_cast(
        father_highest_education_serbia as string
    ) father_highest_education_serbia,
    safe_cast(
        father_highest_education_sweden as string
    ) father_highest_education_sweden,
    safe_cast(
        father_highest_education_slovenia as string
    ) father_highest_education_slovenia,
    safe_cast(
        father_highest_education_slovakia as string
    ) father_highest_education_slovakia,
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
        mother_highest_education_bulgaria as string
    ) mother_highest_education_bulgaria,
    safe_cast(
        mother_highest_education_switzerland as string
    ) mother_highest_education_switzerland,
    safe_cast(
        mother_highest_education_cyprus as string
    ) mother_highest_education_cyprus,
    safe_cast(
        mother_highest_education_czechia as string
    ) mother_highest_education_czechia,
    safe_cast(
        mother_highest_education_germany_3 as string
    ) mother_highest_education_germany_3,
    safe_cast(
        mother_highest_education_germany_4 as string
    ) mother_highest_education_germany_4,
    safe_cast(
        mother_highest_education_germany_5 as string
    ) mother_highest_education_germany_5,
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
        mother_highest_education_united_kingdom as string
    ) mother_highest_education_united_kingdom,
    safe_cast(
        mother_highest_education_united_kingdom_2 as string
    ) mother_highest_education_united_kingdom_2,
    safe_cast(
        mother_age_when_completed_full_time as int64
    ) mother_age_when_completed_full_time,
    safe_cast(
        mother_highest_education_croatia as string
    ) mother_highest_education_croatia,
    safe_cast(
        mother_highest_education_hungary_2 as string
    ) mother_highest_education_hungary_2,
    safe_cast(
        mother_highest_education_ireland as string
    ) mother_highest_education_ireland,
    safe_cast(
        mother_highest_education_iceland as string
    ) mother_highest_education_iceland,
    safe_cast(
        mother_highest_education_italy_2 as string
    ) mother_highest_education_italy_2,
    safe_cast(
        mother_highest_education_lithuania as string
    ) mother_highest_education_lithuania,
    safe_cast(
        mother_highest_education_latvia_2 as string
    ) mother_highest_education_latvia_2,
    safe_cast(
        mother_highest_education_montenegro_2 as string
    ) mother_highest_education_montenegro_2,
    safe_cast(
        mother_highest_education_netherlands as string
    ) mother_highest_education_netherlands,
    safe_cast(
        mother_highest_education_norway_2 as string
    ) mother_highest_education_norway_2,
    safe_cast(
        mother_highest_education_poland_2 as string
    ) mother_highest_education_poland_2,
    safe_cast(
        mother_highest_education_portugal_2 as string
    ) mother_highest_education_portugal_2,
    safe_cast(
        mother_highest_education_serbia as string
    ) mother_highest_education_serbia,
    safe_cast(
        mother_highest_education_sweden as string
    ) mother_highest_education_sweden,
    safe_cast(
        mother_highest_education_slovenia as string
    ) mother_highest_education_slovenia,
    safe_cast(
        mother_highest_education_slovakia as string
    ) mother_highest_education_slovakia,
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
        first_ancestry_european_standard_classification_cultural as string
    ) first_ancestry_european_standard_classification_cultural,
    safe_cast(
        second_ancestry_european_standard_classification_cultural_2 as string
    ) second_ancestry_european_standard_classification_cultural_2,
    safe_cast(
        second_ancestry_european_standard_classification_cultural as string
    ) second_ancestry_european_standard_classification_cultural,
    safe_cast(region_code as string) region_code,
    safe_cast(regional_unit as string) regional_unit,
    safe_cast(
        political_system_country_ensures_everyone_fair as string
    ) political_system_country_ensures_everyone_fair,
    safe_cast(
        government_country_takes_into_account_interests as string
    ) government_country_takes_into_account_interests,
    safe_cast(
        decisions_country_politics_transparent as string
    ) decisions_country_politics_transparent,
    safe_cast(
        compared_other_people_country_fair_chance as int64
    ) compared_other_people_country_fair_chance,
    safe_cast(
        compared_other_people_country_fair_chance_2 as int64
    ) compared_other_people_country_fair_chance_2,
    safe_cast(
        everyone_country_fair_chance_achieve_level as int64
    ) everyone_country_fair_chance_achieve_level,
    safe_cast(
        everyone_country_fair_chance_get_job_they_seek as int64
    ) everyone_country_fair_chance_get_job_they_seek,
    safe_cast(
        filter_variable_ask_pay_pensions_social as string
    ) filter_variable_ask_pay_pensions_social,
    safe_cast(infqbst as string) infqbst,
    safe_cast(what_usual as string) what_usual,
    safe_cast(letter_describes_gross_pay as string) letter_describes_gross_pay,
    safe_cast(usual as string) usual,
    safe_cast(letter_describes_net as string) letter_describes_net,
    safe_cast(
        would_say_gross_pay_unfairly_low as string
    ) would_say_gross_pay_unfairly_low,
    safe_cast(net as string) net,
    safe_cast(fair_level as string) fair_level,
    safe_cast(fair_level_2 as string) fair_level_2,
    safe_cast(net_2 as string) net_2,
    safe_cast(
        top_10_full_time_employees_country as string
    ) top_10_full_time_employees_country,
    safe_cast(
        bottom_10_full_time_employees_country as string
    ) bottom_10_full_time_employees_country,
    safe_cast(
        differences_wealth_country_fair as string
    ) differences_wealth_country_fair,
    safe_cast(
        influence_decision_recruit_person_knowledge_skills as string
    ) influence_decision_recruit_person_knowledge_skills,
    safe_cast(
        influence_decision_recruit_person_job_experience as string
    ) influence_decision_recruit_person_job_experience,
    safe_cast(
        influence_decision_recruit_person_knows_someone as string
    ) influence_decision_recruit_person_knows_someone,
    safe_cast(
        influence_decision_recruit_person_has_immigrant as string
    ) influence_decision_recruit_person_has_immigrant,
    safe_cast(
        influence_decision_recruit_person_gender as string
    ) influence_decision_recruit_person_gender,
    safe_cast(
        society_fair_when_income_wealth_equally as string
    ) society_fair_when_income_wealth_equally,
    safe_cast(
        society_fair_when_hard_working_people as string
    ) society_fair_when_hard_working_people,
    safe_cast(
        society_fair_when_takes_care_poor as string
    ) society_fair_when_takes_care_poor,
    safe_cast(
        society_fair_when_people_from_families as string
    ) society_fair_when_people_from_families,
    safe_cast(
        large_people_get_what_they_deserve as string
    ) large_people_get_what_they_deserve,
    safe_cast(
        confident_justice_always_prevails_over_injustice as string
    ) confident_justice_always_prevails_over_injustice,
    safe_cast(
        convinced_long_run_people_compensated_injustices as string
    ) convinced_long_run_people_compensated_injustices,
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
        interview_length_minutes_main_questionnaire as int64
    ) interview_length_minutes_main_questionnaire,
    safe_cast(sampling_domain as int64) sampling_domain,
    safe_cast(selection_probability as float64) selection_probability,
    safe_cast(sampling_stratum as float64) sampling_stratum,
    safe_cast(primary_sampling_unit as float64) primary_sampling_unit
from {{ set_datalake_project("gb_eric_ess_staging.round_09") }} as t
