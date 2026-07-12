{{
    config(
        schema="gb_eric_ess",
        alias="round_10",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2020, "end": 2025, "interval": 1},
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
    safe_cast(trust_scientists as int64) trust_scientists,
    safe_cast(voted_last_national_election as string) voted_last_national_election,
    safe_cast(party_voted_belgium as string) party_voted_belgium,
    safe_cast(party_voted_bulgaria_2 as string) party_voted_bulgaria_2,
    safe_cast(party_voted_switzerland as string) party_voted_switzerland,
    safe_cast(party_voted_croatia_2 as string) party_voted_croatia_2,
    safe_cast(party_voted_czechia as string) party_voted_czechia,
    safe_cast(party_voted_estonia as string) party_voted_estonia,
    safe_cast(party_voted_finland_2 as string) party_voted_finland_2,
    safe_cast(party_voted_france_ballot_1_2 as string) party_voted_france_ballot_1_2,
    safe_cast(party_voted_greece_2 as string) party_voted_greece_2,
    safe_cast(party_voted_hungary_2 as string) party_voted_hungary_2,
    safe_cast(party_voted_iceland_2 as string) party_voted_iceland_2,
    safe_cast(
        party_voted_ireland_derived_from_1st_2 as string
    ) party_voted_ireland_derived_from_1st_2,
    safe_cast(party_voted_italy_2 as string) party_voted_italy_2,
    safe_cast(
        party_voted_1_lithuania_first_vote_party as string
    ) party_voted_1_lithuania_first_vote_party,
    safe_cast(
        party_voted_2_lithuania_second_vote_party as string
    ) party_voted_2_lithuania_second_vote_party,
    safe_cast(
        party_voted_3_lithuania_third_vote_party as string
    ) party_voted_3_lithuania_third_vote_party,
    safe_cast(party_voted_montenegro_2 as string) party_voted_montenegro_2,
    safe_cast(party_voted_netherlands_2 as string) party_voted_netherlands_2,
    safe_cast(party_voted_north_macedonia as string) party_voted_north_macedonia,
    safe_cast(party_voted_norway_2 as string) party_voted_norway_2,
    safe_cast(party_voted_portugal_2 as string) party_voted_portugal_2,
    safe_cast(party_voted_slovenia_2 as string) party_voted_slovenia_2,
    safe_cast(party_voted_slovakia as string) party_voted_slovakia,
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
    safe_cast(party_feel_close_belgium as string) party_feel_close_belgium,
    safe_cast(party_feel_close_bulgaria_2 as string) party_feel_close_bulgaria_2,
    safe_cast(party_feel_close_switzerland as string) party_feel_close_switzerland,
    safe_cast(party_feel_close_croatia as string) party_feel_close_croatia,
    safe_cast(party_feel_close_czechia as string) party_feel_close_czechia,
    safe_cast(party_feel_close_estonia as string) party_feel_close_estonia,
    safe_cast(party_feel_close_finland_2 as string) party_feel_close_finland_2,
    safe_cast(party_feel_close_france_2 as string) party_feel_close_france_2,
    safe_cast(party_feel_close_greece_2 as string) party_feel_close_greece_2,
    safe_cast(party_feel_close_hungary_2 as string) party_feel_close_hungary_2,
    safe_cast(party_feel_close_iceland_2 as string) party_feel_close_iceland_2,
    safe_cast(party_feel_close_ireland as string) party_feel_close_ireland,
    safe_cast(party_feel_close_italy_2 as string) party_feel_close_italy_2,
    safe_cast(party_feel_close_lithuania as string) party_feel_close_lithuania,
    safe_cast(party_feel_close_montenegro_2 as string) party_feel_close_montenegro_2,
    safe_cast(party_feel_close_netherlands_2 as string) party_feel_close_netherlands_2,
    safe_cast(
        party_feel_close_north_macedonia as string
    ) party_feel_close_north_macedonia,
    safe_cast(party_feel_close_norway_2 as string) party_feel_close_norway_2,
    safe_cast(party_feel_close_portugal_2 as string) party_feel_close_portugal_2,
    safe_cast(party_feel_close_slovenia_2 as string) party_feel_close_slovenia_2,
    safe_cast(party_feel_close_slovakia as string) party_feel_close_slovakia,
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
        religion_denomination_belonging_present_switzerland as string
    ) religion_denomination_belonging_present_switzerland,
    safe_cast(
        religion_denomination_belonging_present_finland as string
    ) religion_denomination_belonging_present_finland,
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
        religion_denomination_belonging_present_lithuania as string
    ) religion_denomination_belonging_present_lithuania,
    safe_cast(
        religion_denomination_belonging_present_montenegro as string
    ) religion_denomination_belonging_present_montenegro,
    safe_cast(
        religion_denomination_belonging_present_netherlands as string
    ) religion_denomination_belonging_present_netherlands,
    safe_cast(
        religion_denomination_belonging_present_north_macedonia as string
    ) religion_denomination_belonging_present_north_macedonia,
    safe_cast(
        religion_denomination_belonging_present_norway as string
    ) religion_denomination_belonging_present_norway,
    safe_cast(
        religion_denomination_belonging_present_slovakia_2 as string
    ) religion_denomination_belonging_present_slovakia_2,
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
        religion_denomination_belonging_past_switzerland as string
    ) religion_denomination_belonging_past_switzerland,
    safe_cast(
        religion_denomination_belonging_past_finland as string
    ) religion_denomination_belonging_past_finland,
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
        religion_denomination_belonging_past_lithuania as string
    ) religion_denomination_belonging_past_lithuania,
    safe_cast(
        religion_denomination_belonging_past_montenegro as string
    ) religion_denomination_belonging_past_montenegro,
    safe_cast(
        religion_denomination_belonging_past_netherlands as string
    ) religion_denomination_belonging_past_netherlands,
    safe_cast(
        religion_denomination_belonging_past_north_macedonia as string
    ) religion_denomination_belonging_past_north_macedonia,
    safe_cast(
        religion_denomination_belonging_past_norway as string
    ) religion_denomination_belonging_past_norway,
    safe_cast(
        religion_denomination_belonging_past_slovakia_2 as string
    ) religion_denomination_belonging_past_slovakia_2,
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
        imagine_large_numbers_people_limit_energy_7 as int64
    ) imagine_large_numbers_people_limit_energy_7,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_7 as int64
    ) likelihood_large_numbers_people_limit_energy_use_7,
    safe_cast(
        likelihood_governments_enough_countries_take_action_7 as int64
    ) likelihood_governments_enough_countries_take_action_7,
    safe_cast(
        imagine_large_numbers_people_limit_energy_8 as string
    ) imagine_large_numbers_people_limit_energy_8,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_8 as string
    ) likelihood_large_numbers_people_limit_energy_use_8,
    safe_cast(
        likelihood_governments_enough_countries_take_action_8 as string
    ) likelihood_governments_enough_countries_take_action_8,
    safe_cast(
        imagine_large_numbers_people_limit_energy_9 as string
    ) imagine_large_numbers_people_limit_energy_9,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_9 as string
    ) likelihood_large_numbers_people_limit_energy_use_9,
    safe_cast(
        likelihood_governments_enough_countries_take_action_9 as string
    ) likelihood_governments_enough_countries_take_action_9,
    safe_cast(would_vote as string) would_vote,
    safe_cast(would_vote_2 as string) would_vote_2,
    safe_cast(national_elections_free_fair as int64) national_elections_free_fair,
    safe_cast(
        different_political_parties_offer_clear_alternatives as int64
    ) different_political_parties_offer_clear_alternatives,
    safe_cast(media_free_criticise_government as int64) media_free_criticise_government,
    safe_cast(
        rights_minority_groups_protected as int64
    ) rights_minority_groups_protected,
    safe_cast(
        citizens_have_final_say_political_issues as int64
    ) citizens_have_final_say_political_issues,
    safe_cast(courts_treat_everyone_same as int64) courts_treat_everyone_same,
    safe_cast(
        governing_parties_punished_elections_when_they as int64
    ) governing_parties_punished_elections_when_they,
    safe_cast(
        government_protects_all_citizens_against_poverty as int64
    ) government_protects_all_citizens_against_poverty,
    safe_cast(
        government_takes_measures_reduce_differences_income as int64
    ) government_takes_measures_reduce_differences_income,
    safe_cast(
        views_ordinary_people_prevail_over_views as int64
    ) views_ordinary_people_prevail_over_views,
    safe_cast(will_people_cannot_stopped as int64) will_people_cannot_stopped,
    safe_cast(
        key_decisions_made_national_governments_rather as int64
    ) key_decisions_made_national_governments_rather,
    safe_cast(
        country_national_elections_free_fair as int64
    ) country_national_elections_free_fair,
    safe_cast(
        country_different_political_parties_offer_clear as int64
    ) country_different_political_parties_offer_clear,
    safe_cast(
        country_media_free_criticise_government as int64
    ) country_media_free_criticise_government,
    safe_cast(
        country_rights_minority_groups_protected as int64
    ) country_rights_minority_groups_protected,
    safe_cast(
        country_citizens_have_final_say_political as int64
    ) country_citizens_have_final_say_political,
    safe_cast(
        country_courts_treat_everyone_same as int64
    ) country_courts_treat_everyone_same,
    safe_cast(
        country_governing_parties_punished_elections_when as int64
    ) country_governing_parties_punished_elections_when,
    safe_cast(
        country_government_protects_all_citizens_against as int64
    ) country_government_protects_all_citizens_against,
    safe_cast(
        country_government_takes_measures_reduce_differences as int64
    ) country_government_takes_measures_reduce_differences,
    safe_cast(
        country_views_ordinary_people_prevail_over as int64
    ) country_views_ordinary_people_prevail_over,
    safe_cast(
        country_will_people_cannot_stopped as int64
    ) country_will_people_cannot_stopped,
    safe_cast(
        country_key_decisions_made_national_governments as int64
    ) country_key_decisions_made_national_governments,
    safe_cast(
        best_democracy_government_changes_policies_response as string
    ) best_democracy_government_changes_policies_response,
    safe_cast(
        important_democracy_government_changes_policies_response as int64
    ) important_democracy_government_changes_policies_response,
    safe_cast(
        country_government_changes_policies_response_what as int64
    ) country_government_changes_policies_response_what,
    safe_cast(
        important_democracy_government_sticks_policies_regardless as int64
    ) important_democracy_government_sticks_policies_regardless,
    safe_cast(
        country_government_sticks_policies_regardless_what as int64
    ) country_government_sticks_policies_regardless_what,
    safe_cast(
        administration_important_things_democracy as string
    ) administration_important_things_democracy,
    safe_cast(using_correct_version_showcard as string) using_correct_version_showcard,
    safe_cast(
        important_things_democracy_order as string
    ) important_things_democracy_order,
    safe_cast(
        important_things_democracy_order_b as string
    ) important_things_democracy_order_b,
    safe_cast(
        important_things_democracy_order_c as string
    ) important_things_democracy_order_c,
    safe_cast(
        important_things_democracy_order_d as string
    ) important_things_democracy_order_d,
    safe_cast(
        important_things_democracy_order_e as string
    ) important_things_democracy_order_e,
    safe_cast(
        important_live_democratically_governed_country as int64
    ) important_live_democratically_governed_country,
    safe_cast(
        acceptable_country_have_strong_leader_above_law as int64
    ) acceptable_country_have_strong_leader_above_law,
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
        relationship_husband_wife_partner_currently_living as string
    ) relationship_husband_wife_partner_currently_living,
    safe_cast(
        relationship_husband_wife_partner_currently_living_4 as string
    ) relationship_husband_wife_partner_currently_living_4,
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
        legal_marital_status_north_macedonia as string
    ) legal_marital_status_north_macedonia,
    safe_cast(
        legal_marital_status_united_kingdom as string
    ) legal_marital_status_united_kingdom,
    safe_cast(legal_marital_status_2 as string) legal_marital_status_2,
    safe_cast(children_living_at_home as string) children_living_at_home,
    safe_cast(domicile_type as string) domicile_type,
    safe_cast(highest_education as string) highest_education,
    safe_cast(highest_education_es_isced as string) highest_education_es_isced,
    safe_cast(highest_education_belgium as string) highest_education_belgium,
    safe_cast(highest_education_bulgaria as string) highest_education_bulgaria,
    safe_cast(highest_education_switzerland as string) highest_education_switzerland,
    safe_cast(highest_education_croatia as string) highest_education_croatia,
    safe_cast(highest_education_czechia as string) highest_education_czechia,
    safe_cast(highest_education_estonia as string) highest_education_estonia,
    safe_cast(highest_education_finland as string) highest_education_finland,
    safe_cast(highest_education_france as string) highest_education_france,
    safe_cast(highest_education_greece as string) highest_education_greece,
    safe_cast(highest_education_hungary as string) highest_education_hungary,
    safe_cast(highest_education_iceland as string) highest_education_iceland,
    safe_cast(highest_education_ireland as string) highest_education_ireland,
    safe_cast(highest_education_italy_2 as string) highest_education_italy_2,
    safe_cast(highest_education_lithuania as string) highest_education_lithuania,
    safe_cast(highest_education_montenegro_2 as string) highest_education_montenegro_2,
    safe_cast(highest_education_netherlands as string) highest_education_netherlands,
    safe_cast(
        highest_education_north_macedonia as string
    ) highest_education_north_macedonia,
    safe_cast(highest_education_norway as string) highest_education_norway,
    safe_cast(highest_education_portugal_2 as string) highest_education_portugal_2,
    safe_cast(highest_education_slovenia as string) highest_education_slovenia,
    safe_cast(highest_education_slovakia as string) highest_education_slovakia,
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
        partner_highest_education_belgium as string
    ) partner_highest_education_belgium,
    safe_cast(
        partner_highest_education_bulgaria as string
    ) partner_highest_education_bulgaria,
    safe_cast(
        partner_highest_education_switzerland as string
    ) partner_highest_education_switzerland,
    safe_cast(
        partner_highest_education_croatia as string
    ) partner_highest_education_croatia,
    safe_cast(
        partner_highest_education_czechia as string
    ) partner_highest_education_czechia,
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
        partner_highest_education_italy_2 as string
    ) partner_highest_education_italy_2,
    safe_cast(
        partner_highest_education_lithuania as string
    ) partner_highest_education_lithuania,
    safe_cast(
        partner_highest_education_montenegro_2 as string
    ) partner_highest_education_montenegro_2,
    safe_cast(
        partner_highest_education_netherlands as string
    ) partner_highest_education_netherlands,
    safe_cast(
        partner_highest_education_north_macedonia as string
    ) partner_highest_education_north_macedonia,
    safe_cast(
        partner_highest_education_norway as string
    ) partner_highest_education_norway,
    safe_cast(
        partner_highest_education_portugal_2 as string
    ) partner_highest_education_portugal_2,
    safe_cast(
        partner_highest_education_slovenia as string
    ) partner_highest_education_slovenia,
    safe_cast(
        partner_highest_education_slovakia as string
    ) partner_highest_education_slovakia,
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
        father_highest_education_belgium as string
    ) father_highest_education_belgium,
    safe_cast(
        father_highest_education_bulgaria as string
    ) father_highest_education_bulgaria,
    safe_cast(
        father_highest_education_switzerland as string
    ) father_highest_education_switzerland,
    safe_cast(
        father_highest_education_croatia as string
    ) father_highest_education_croatia,
    safe_cast(
        father_highest_education_czechia as string
    ) father_highest_education_czechia,
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
        father_highest_education_italy_2 as string
    ) father_highest_education_italy_2,
    safe_cast(
        father_highest_education_lithuania as string
    ) father_highest_education_lithuania,
    safe_cast(
        father_highest_education_montenegro_2 as string
    ) father_highest_education_montenegro_2,
    safe_cast(
        father_highest_education_netherlands as string
    ) father_highest_education_netherlands,
    safe_cast(
        father_highest_education_north_macedonia as string
    ) father_highest_education_north_macedonia,
    safe_cast(
        father_highest_education_norway as string
    ) father_highest_education_norway,
    safe_cast(
        father_highest_education_portugal_2 as string
    ) father_highest_education_portugal_2,
    safe_cast(
        father_highest_education_slovenia as string
    ) father_highest_education_slovenia,
    safe_cast(
        father_highest_education_slovakia as string
    ) father_highest_education_slovakia,
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
        mother_highest_education_belgium as string
    ) mother_highest_education_belgium,
    safe_cast(
        mother_highest_education_bulgaria as string
    ) mother_highest_education_bulgaria,
    safe_cast(
        mother_highest_education_switzerland as string
    ) mother_highest_education_switzerland,
    safe_cast(
        mother_highest_education_croatia as string
    ) mother_highest_education_croatia,
    safe_cast(
        mother_highest_education_czechia as string
    ) mother_highest_education_czechia,
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
        mother_highest_education_italy_2 as string
    ) mother_highest_education_italy_2,
    safe_cast(
        mother_highest_education_lithuania as string
    ) mother_highest_education_lithuania,
    safe_cast(
        mother_highest_education_montenegro_2 as string
    ) mother_highest_education_montenegro_2,
    safe_cast(
        mother_highest_education_netherlands as string
    ) mother_highest_education_netherlands,
    safe_cast(
        mother_highest_education_north_macedonia as string
    ) mother_highest_education_north_macedonia,
    safe_cast(
        mother_highest_education_norway as string
    ) mother_highest_education_norway,
    safe_cast(
        mother_highest_education_portugal_2 as string
    ) mother_highest_education_portugal_2,
    safe_cast(
        mother_highest_education_slovenia as string
    ) mother_highest_education_slovenia,
    safe_cast(
        mother_highest_education_slovakia as string
    ) mother_highest_education_slovakia,
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
    safe_cast(regional_unit as string) regional_unit,
    safe_cast(region_code as string) region_code,
    safe_cast(location_able_access_home as string) location_able_access_home,
    safe_cast(location_able_access_workplace as string) location_able_access_workplace,
    safe_cast(location_able_access_move as string) location_able_access_move,
    safe_cast(
        location_able_access_some_other_place as string
    ) location_able_access_some_other_place,
    safe_cast(
        location_able_access_none_above as string
    ) location_able_access_none_above,
    safe_cast(location_able_access_refusal as string) location_able_access_refusal,
    safe_cast(location_able_access_dont_know as string) location_able_access_dont_know,
    safe_cast(location_able_access_no_answer as string) location_able_access_no_answer,
    safe_cast(preference_settings_familiar as string) preference_settings_familiar,
    safe_cast(advanced_search_familiar as string) advanced_search_familiar,
    safe_cast(pdf_familiar as string) pdf_familiar,
    safe_cast(
        online_mobile_communication_makes_people_feel as int64
    ) online_mobile_communication_makes_people_feel,
    safe_cast(
        online_mobile_communication_makes_work_personal as int64
    ) online_mobile_communication_makes_work_personal,
    safe_cast(
        online_mobile_communication_makes_it_easy as int64
    ) online_mobile_communication_makes_it_easy,
    safe_cast(
        online_mobile_communication_undermines_personal_privacy as int64
    ) online_mobile_communication_undermines_personal_privacy,
    safe_cast(
        online_mobile_communication_exposes_people_misinformation as int64
    ) online_mobile_communication_exposes_people_misinformation,
    safe_cast(number_children_aged_12_over as string) number_children_aged_12_over,
    safe_cast(gender_child_aged_12_over as string) gender_child_aged_12_over,
    safe_cast(gender_child_aged_12_over_2 as string) gender_child_aged_12_over_2,
    safe_cast(age_child_aged_12_over as string) age_child_aged_12_over,
    safe_cast(
        child_aged_12_over_lives_same_household as string
    ) child_aged_12_over_lives_same_household,
    safe_cast(close_child_aged_12_over as string) close_child_aged_12_over,
    safe_cast(
        travel_time_child_aged_12_over_minutes as string
    ) travel_time_child_aged_12_over_minutes,
    safe_cast(
        speak_child_aged_12_over_person_frequency as string
    ) speak_child_aged_12_over_person_frequency,
    safe_cast(speak_child_aged_12_over_see as string) speak_child_aged_12_over_see,
    safe_cast(
        speak_child_aged_12_over_using_phone_frequency as string
    ) speak_child_aged_12_over_using_phone_frequency,
    safe_cast(
        communicate_child_aged_12_over_via as string
    ) communicate_child_aged_12_over_via,
    safe_cast(
        speak_child_aged_12_over_person as string
    ) speak_child_aged_12_over_person,
    safe_cast(
        online_mobile_communication_child_aged_12 as string
    ) online_mobile_communication_child_aged_12,
    safe_cast(parents_still_alive as string) parents_still_alive,
    safe_cast(
        parents_still_alive_mother_father as string
    ) parents_still_alive_mother_father,
    safe_cast(age_parent as string) age_parent,
    safe_cast(parent_lives_same_household as string) parent_lives_same_household,
    safe_cast(close_parent as string) close_parent,
    safe_cast(travel_time_parent_minutes as string) travel_time_parent_minutes,
    safe_cast(speak_parent_person_frequency as string) speak_parent_person_frequency,
    safe_cast(
        speak_parent_see_each_other_screen_frequency as string
    ) speak_parent_see_each_other_screen_frequency,
    safe_cast(
        speak_parent_using_phone_frequency as string
    ) speak_parent_using_phone_frequency,
    safe_cast(
        communicate_parent_via_text_email_messaging as string
    ) communicate_parent_via_text_email_messaging,
    safe_cast(
        speak_parent_person_frequency_compared_before as string
    ) speak_parent_person_frequency_compared_before,
    safe_cast(
        online_mobile_communication_parent_frequency_compared as string
    ) online_mobile_communication_parent_frequency_compared,
    safe_cast(satisfied_main_job as int64) satisfied_main_job,
    safe_cast(
        too_tired_after_work_enjoy_things as string
    ) too_tired_after_work_enjoy_things,
    safe_cast(
        job_prevents_from_giving_time_partner as string
    ) job_prevents_from_giving_time_partner,
    safe_cast(
        partner_family_fed_up_pressure_job_frequency as string
    ) partner_family_fed_up_pressure_job_frequency,
    safe_cast(
        current_job_can_decide_time_start as string
    ) current_job_can_decide_time_start,
    safe_cast(
        work_from_home_place_choice_frequency as string
    ) work_from_home_place_choice_frequency,
    safe_cast(
        work_from_home_place_choice_frequency_2 as string
    ) work_from_home_place_choice_frequency_2,
    safe_cast(
        work_place_change_occurred_result_covid_19 as string
    ) work_place_change_occurred_result_covid_19,
    safe_cast(
        employees_expected_work_overtime_frequency as string
    ) employees_expected_work_overtime_frequency,
    safe_cast(
        employees_expected_responsive_outside_working_hours as string
    ) employees_expected_responsive_outside_working_hours,
    safe_cast(
        work_from_home_place_choice_accepted as string
    ) work_from_home_place_choice_accepted,
    safe_cast(
        line_manager_supports_employees_balancing_work as int64
    ) line_manager_supports_employees_balancing_work,
    safe_cast(
        line_manager_gives_work_related_help_likelihood as string
    ) line_manager_gives_work_related_help_likelihood,
    safe_cast(
        line_manager_respondent_same_workplace_frequency as string
    ) line_manager_respondent_same_workplace_frequency,
    safe_cast(
        speak_line_manager_work_person_frequency as string
    ) speak_line_manager_work_person_frequency,
    safe_cast(
        speak_line_manager_work_see_each as string
    ) speak_line_manager_work_see_each,
    safe_cast(
        speak_line_work_manager_using_phone_frequency as string
    ) speak_line_work_manager_using_phone_frequency,
    safe_cast(
        communicate_line_manager_work_via_text as string
    ) communicate_line_manager_work_via_text,
    safe_cast(feel_like_part_team_much as int64) feel_like_part_team_much,
    safe_cast(
        take_extra_responsibilities_work_without_paid as int64
    ) take_extra_responsibilities_work_without_paid,
    safe_cast(
        proportion_colleagues_based_same_location as string
    ) proportion_colleagues_based_same_location,
    safe_cast(
        colleagues_give_work_related_help_likelihood as string
    ) colleagues_give_work_related_help_likelihood,
    safe_cast(
        speak_colleagues_person_frequency as string
    ) speak_colleagues_person_frequency,
    safe_cast(
        speak_colleagues_work_see_each_other as string
    ) speak_colleagues_work_see_each_other,
    safe_cast(
        speak_colleagues_work_using_phone_frequency as string
    ) speak_colleagues_work_using_phone_frequency,
    safe_cast(
        communicate_colleagues_work_via_text_email as string
    ) communicate_colleagues_work_via_text_email,
    safe_cast(
        speak_people_work_person_frequency_compared as string
    ) speak_people_work_person_frequency_compared,
    safe_cast(
        online_mobile_communication_people_work_frequency as string
    ) online_mobile_communication_people_work_frequency,
    safe_cast(
        online_mobile_communication_makes_it_easy_2 as int64
    ) online_mobile_communication_makes_it_easy_2,
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
        imagine_large_numbers_people_limit_energy_10 as string
    ) imagine_large_numbers_people_limit_energy_10,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_10 as string
    ) likelihood_large_numbers_people_limit_energy_use_10,
    safe_cast(
        likelihood_governments_enough_countries_take_action_10 as string
    ) likelihood_governments_enough_countries_take_action_10,
    safe_cast(
        imagine_large_numbers_people_limit_energy_11 as string
    ) imagine_large_numbers_people_limit_energy_11,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_11 as string
    ) likelihood_large_numbers_people_limit_energy_use_11,
    safe_cast(
        likelihood_governments_enough_countries_take_action_11 as string
    ) likelihood_governments_enough_countries_take_action_11,
    safe_cast(
        imagine_large_numbers_people_limit_energy_12 as int64
    ) imagine_large_numbers_people_limit_energy_12,
    safe_cast(
        likelihood_large_numbers_people_limit_energy_use_12 as int64
    ) likelihood_large_numbers_people_limit_energy_use_12,
    safe_cast(
        likelihood_governments_enough_countries_take_action_12 as int64
    ) likelihood_governments_enough_countries_take_action_12,
    safe_cast(
        small_secret_group_people_responsible_making as string
    ) small_secret_group_people_responsible_making,
    safe_cast(
        groups_scientists_manipulate_fabricate_suppress_evidence as string
    ) groups_scientists_manipulate_fabricate_suppress_evidence,
    safe_cast(
        administration_covid_19_questions as string
    ) administration_covid_19_questions,
    safe_cast(
        more_important_governments_prioritise_public_health as int64
    ) more_important_governments_prioritise_public_health,
    safe_cast(
        more_important_governments_monitor_track_public as int64
    ) more_important_governments_monitor_track_public,
    safe_cast(
        more_important_governments_prioritise_public_health_2 as int64
    ) more_important_governments_prioritise_public_health_2,
    safe_cast(
        more_important_governments_monitor_track_public_2 as int64
    ) more_important_governments_monitor_track_public_2,
    safe_cast(
        more_important_follow_government_rules_make as int64
    ) more_important_follow_government_rules_make,
    safe_cast(
        important_country_close_its_borders_when as int64
    ) important_country_close_its_borders_when,
    safe_cast(
        important_restrict_people_movement_between_different as int64
    ) important_restrict_people_movement_between_different,
    safe_cast(
        satisfied_government_handling_covid_19_country as int64
    ) satisfied_government_handling_covid_19_country,
    safe_cast(
        satisfied_government_response_people_who_have as int64
    ) satisfied_government_response_people_who_have,
    safe_cast(
        satisfied_government_response_elderly_people_care as int64
    ) satisfied_government_response_elderly_people_care,
    safe_cast(
        satisfied_government_response_families_school as int64
    ) satisfied_government_response_families_school,
    safe_cast(
        satisfied_way_health_services_coped_covid_19_its as int64
    ) satisfied_way_health_services_coped_covid_19_its,
    safe_cast(
        government_balanced_protecting_economy_people_health as int64
    ) government_balanced_protecting_economy_people_health,
    safe_cast(
        what_extent_trust_government_deal_impact as int64
    ) what_extent_trust_government_deal_impact,
    safe_cast(
        covid_19_result_deliberate_concealed_efforts as string
    ) covid_19_result_deliberate_concealed_efforts,
    safe_cast(respondent_had_covid_19 as string) respondent_had_covid_19,
    safe_cast(
        anyone_living_respondent_had_covid_19 as string
    ) anyone_living_respondent_had_covid_19,
    safe_cast(
        things_happened_since_made_redundant_lost_job as string
    ) things_happened_since_made_redundant_lost_job,
    safe_cast(
        things_happened_since_income_from_job_reduced as string
    ) things_happened_since_income_from_job_reduced,
    safe_cast(
        things_happened_since_working_hours_were_reduced as string
    ) things_happened_since_working_hours_were_reduced,
    safe_cast(
        things_happened_since_furloughed as string
    ) things_happened_since_furloughed,
    safe_cast(
        things_happened_since_forced_take_unpaid_leave as string
    ) things_happened_since_forced_take_unpaid_leave,
    safe_cast(
        things_happened_since_none_these as string
    ) things_happened_since_none_these,
    safe_cast(
        things_happened_since_not_work_since_start as string
    ) things_happened_since_not_work_since_start,
    safe_cast(
        things_happened_since_not_applicable as string
    ) things_happened_since_not_applicable,
    safe_cast(things_happened_since_refusal as string) things_happened_since_refusal,
    safe_cast(
        things_happened_since_dont_know as string
    ) things_happened_since_dont_know,
    safe_cast(
        things_happened_since_no_answer as string
    ) things_happened_since_no_answer,
    safe_cast(
        covid_19_vaccine_approved_national_regulatory as string
    ) covid_19_vaccine_approved_national_regulatory,
    safe_cast(
        whether_respondent_will_get_vaccinated_against as string
    ) whether_respondent_will_get_vaccinated_against,
    safe_cast(
        whether_respondent_would_get_vaccinated_against as string
    ) whether_respondent_would_get_vaccinated_against,
    safe_cast(interview_conducted as string) interview_conducted,
    safe_cast(
        respondent_overall_experience_taking_part_survey as int64
    ) respondent_overall_experience_taking_part_survey,
    safe_cast(
        type_device_used_video_interview as string
    ) type_device_used_video_interview,
    safe_cast(
        respondent_experience_technical_starting_video_call as string
    ) respondent_experience_technical_starting_video_call,
    safe_cast(
        respondent_experience_technical_internet_connection_affecting as string
    ) respondent_experience_technical_internet_connection_affecting,
    safe_cast(
        respondent_experience_technical_displaying_showcards as string
    ) respondent_experience_technical_displaying_showcards,
    safe_cast(
        respondent_experience_technical_audio_not_clear as string
    ) respondent_experience_technical_audio_not_clear,
    safe_cast(
        respondent_experience_technical_video_display_not as string
    ) respondent_experience_technical_video_display_not,
    safe_cast(
        respondent_experience_technical_other_issue as string
    ) respondent_experience_technical_other_issue,
    safe_cast(
        respondent_experience_technical_no_technical_issues as string
    ) respondent_experience_technical_no_technical_issues,
    safe_cast(
        respondent_experience_technical_not_applicable as string
    ) respondent_experience_technical_not_applicable,
    safe_cast(
        respondent_experience_technical_refusal as string
    ) respondent_experience_technical_refusal,
    safe_cast(
        respondent_experience_technical_dont_know as string
    ) respondent_experience_technical_dont_know,
    safe_cast(
        respondent_experience_technical_no_answer as string
    ) respondent_experience_technical_no_answer,
    safe_cast(start_interview as string) start_interview,
    safe_cast(start_section as string) start_section,
    safe_cast(end_section as string) end_section,
    safe_cast(end_section_b as string) end_section_b,
    safe_cast(end_section_c as string) end_section_c,
    safe_cast(end_section_d as string) end_section_d,
    safe_cast(end_section_f as string) end_section_f,
    safe_cast(end_section_g as float64) end_section_g,
    safe_cast(end_section_h as string) end_section_h,
    safe_cast(end_section_i as string) end_section_i,
    safe_cast(end_section_k as string) end_section_k,
    safe_cast(end_section_v as float64) end_section_v,
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
from {{ set_datalake_project("gb_eric_ess_staging.round_10") }} as t
