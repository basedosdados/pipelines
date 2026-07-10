{{
    config(
        schema="eu_ess",
        alias="round_02",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2009, "interval": 1},
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
    safe_cast(party_voted_austria_5 as string) party_voted_austria_5,
    safe_cast(party_voted_belgium_5 as string) party_voted_belgium_5,
    safe_cast(party_voted_switzerland_9 as string) party_voted_switzerland_9,
    safe_cast(party_voted_czechia_6 as string) party_voted_czechia_6,
    safe_cast(party_voted_1_germany_6 as string) party_voted_1_germany_6,
    safe_cast(party_voted_2_germany_6 as string) party_voted_2_germany_6,
    safe_cast(party_voted_denmark_5 as string) party_voted_denmark_5,
    safe_cast(party_voted_estonia_9 as string) party_voted_estonia_9,
    safe_cast(party_voted_spain_6 as string) party_voted_spain_6,
    safe_cast(party_voted_finland_7 as string) party_voted_finland_7,
    safe_cast(party_voted_france_ballot_1_7 as string) party_voted_france_ballot_1_7,
    safe_cast(party_voted_united_kingdom_4 as string) party_voted_united_kingdom_4,
    safe_cast(party_voted_greece_5 as string) party_voted_greece_5,
    safe_cast(party_voted_hungary_9 as string) party_voted_hungary_9,
    safe_cast(party_voted_ireland_3 as string) party_voted_ireland_3,
    safe_cast(party_voted_iceland_6 as string) party_voted_iceland_6,
    safe_cast(party_voted_italy_4 as string) party_voted_italy_4,
    safe_cast(party_voted_luxembourg as string) party_voted_luxembourg,
    safe_cast(party_voted_netherlands_9 as string) party_voted_netherlands_9,
    safe_cast(party_voted_norway_4 as string) party_voted_norway_4,
    safe_cast(party_voted_poland_6 as string) party_voted_poland_6,
    safe_cast(party_voted_portugal_6 as string) party_voted_portugal_6,
    safe_cast(party_voted_sweden_5 as string) party_voted_sweden_5,
    safe_cast(party_voted_slovenia_7 as string) party_voted_slovenia_7,
    safe_cast(party_voted_slovakia_6 as string) party_voted_slovakia_6,
    safe_cast(party_voted_turkey_2 as string) party_voted_turkey_2,
    safe_cast(party_voted_ukraine_3 as string) party_voted_ukraine_3,
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
    safe_cast(party_feel_close_austria_5 as string) party_feel_close_austria_5,
    safe_cast(party_feel_close_belgium_5 as string) party_feel_close_belgium_5,
    safe_cast(party_feel_close_switzerland_9 as string) party_feel_close_switzerland_9,
    safe_cast(party_feel_close_czechia_6 as string) party_feel_close_czechia_6,
    safe_cast(party_feel_close_germany_6 as string) party_feel_close_germany_6,
    safe_cast(party_feel_close_denmark_5 as string) party_feel_close_denmark_5,
    safe_cast(party_feel_close_estonia_9 as string) party_feel_close_estonia_9,
    safe_cast(party_feel_close_spain_7 as string) party_feel_close_spain_7,
    safe_cast(party_feel_close_finland_8 as string) party_feel_close_finland_8,
    safe_cast(party_feel_close_france_8 as string) party_feel_close_france_8,
    safe_cast(
        party_feel_close_united_kingdom_4 as string
    ) party_feel_close_united_kingdom_4,
    safe_cast(party_feel_close_greece_5 as string) party_feel_close_greece_5,
    safe_cast(party_feel_close_hungary_10 as string) party_feel_close_hungary_10,
    safe_cast(party_feel_close_ireland_7 as string) party_feel_close_ireland_7,
    safe_cast(party_feel_close_iceland_6 as string) party_feel_close_iceland_6,
    safe_cast(party_feel_close_italy_5 as string) party_feel_close_italy_5,
    safe_cast(party_feel_close_luxembourg as string) party_feel_close_luxembourg,
    safe_cast(party_feel_close_netherlands_9 as string) party_feel_close_netherlands_9,
    safe_cast(party_feel_close_norway_4 as string) party_feel_close_norway_4,
    safe_cast(party_feel_close_poland_9 as string) party_feel_close_poland_9,
    safe_cast(party_feel_close_portugal_7 as string) party_feel_close_portugal_7,
    safe_cast(party_feel_close_sweden_5 as string) party_feel_close_sweden_5,
    safe_cast(party_feel_close_slovenia_7 as string) party_feel_close_slovenia_7,
    safe_cast(party_feel_close_slovakia_6 as string) party_feel_close_slovakia_6,
    safe_cast(party_feel_close_turkey_2 as string) party_feel_close_turkey_2,
    safe_cast(party_feel_close_ukraine_5 as string) party_feel_close_ukraine_5,
    safe_cast(how_close_to_party as string) how_close_to_party,
    safe_cast(member_political_party as string) member_political_party,
    safe_cast(member_party_austria_2 as string) member_party_austria_2,
    safe_cast(member_party_belgium_3 as string) member_party_belgium_3,
    safe_cast(member_party_switzerland_4 as string) member_party_switzerland_4,
    safe_cast(member_party_czechia_3 as string) member_party_czechia_3,
    safe_cast(member_party_denmark_3 as string) member_party_denmark_3,
    safe_cast(member_party_germany_3 as string) member_party_germany_3,
    safe_cast(member_party_estonia_4 as string) member_party_estonia_4,
    safe_cast(member_party_spain_2 as string) member_party_spain_2,
    safe_cast(member_party_finland_3 as string) member_party_finland_3,
    safe_cast(member_party_france_4 as string) member_party_france_4,
    safe_cast(member_party_united_kingdom as string) member_party_united_kingdom,
    safe_cast(member_party_greece_3 as string) member_party_greece_3,
    safe_cast(member_party_hungary_4 as string) member_party_hungary_4,
    safe_cast(member_party_ireland_3 as string) member_party_ireland_3,
    safe_cast(member_party_iceland as string) member_party_iceland,
    safe_cast(member_party_italy as string) member_party_italy,
    safe_cast(member_party_luxembourg as string) member_party_luxembourg,
    safe_cast(member_party_netherlands_4 as string) member_party_netherlands_4,
    safe_cast(member_party_norway_2 as string) member_party_norway_2,
    safe_cast(member_party_poland_4 as string) member_party_poland_4,
    safe_cast(member_party_portugal_3 as string) member_party_portugal_3,
    safe_cast(member_party_sweden_2 as string) member_party_sweden_2,
    safe_cast(member_party_slovenia_3 as string) member_party_slovenia_3,
    safe_cast(member_party_slovakia_3 as string) member_party_slovakia_3,
    safe_cast(member_party_turkey_2 as string) member_party_turkey_2,
    safe_cast(member_party_ukraine_4 as string) member_party_ukraine_4,
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
        approve_if_healthy_people_use_medicines as string
    ) approve_if_healthy_people_use_medicines,
    safe_cast(
        approve_if_healthy_people_use_medicines_2 as string
    ) approve_if_healthy_people_use_medicines_2,
    safe_cast(
        approve_if_healthy_people_use_medicines_3 as string
    ) approve_if_healthy_people_use_medicines_3,
    safe_cast(
        approve_if_healthy_people_use_medicines_4 as string
    ) approve_if_healthy_people_use_medicines_4,
    safe_cast(
        approve_if_healthy_people_use_medicines_5 as string
    ) approve_if_healthy_people_use_medicines_5,
    safe_cast(
        use_herbal_remedies_if_health_problem_frequency as string
    ) use_herbal_remedies_if_health_problem_frequency,
    safe_cast(
        when_prescribed_medicine_frequency_worry_side as string
    ) when_prescribed_medicine_frequency_worry_side,
    safe_cast(
        when_health_problem_frequency_prefer_medicine as string
    ) when_health_problem_frequency_prefer_medicine,
    safe_cast(
        did_time_doctor_prescribed_medicine_not as string
    ) did_time_doctor_prescribed_medicine_not,
    safe_cast(
        regularly_taking_pills_use_medication_prescribed as string
    ) regularly_taking_pills_use_medication_prescribed,
    safe_cast(
        used_medicine_prescribed_someone_else_frequency as string
    ) used_medicine_prescribed_someone_else_frequency,
    safe_cast(
        who_would_go_first_advise_treatment as string
    ) who_would_go_first_advise_treatment,
    safe_cast(
        other_practitioner_go_first_advise_treatment as string
    ) other_practitioner_go_first_advise_treatment,
    safe_cast(
        who_would_go_first_advise_treatment_2 as string
    ) who_would_go_first_advise_treatment_2,
    safe_cast(
        other_practitioner_first_advise_treatment_if as string
    ) other_practitioner_first_advise_treatment_if,
    safe_cast(
        who_would_go_first_advise_treatment_3 as string
    ) who_would_go_first_advise_treatment_3,
    safe_cast(
        other_practitioner_first_advise_treatment_if_2 as string
    ) other_practitioner_first_advise_treatment_if_2,
    safe_cast(
        who_would_go_first_advise_treatment_4 as string
    ) who_would_go_first_advise_treatment_4,
    safe_cast(
        other_practitioner_first_advise_treatment_if_3 as string
    ) other_practitioner_first_advise_treatment_if_3,
    safe_cast(
        feel_have_choice_choosing_regular_general as string
    ) feel_have_choice_choosing_regular_general,
    safe_cast(
        prefer_same_doctor_all_everyday_health_problems as string
    ) prefer_same_doctor_all_everyday_health_problems,
    safe_cast(
        consulted_doctor_specialist_gp_number_times as string
    ) consulted_doctor_specialist_gp_number_times,
    safe_cast(
        most_illnesses_cure_themselves_without_having as string
    ) most_illnesses_cure_themselves_without_having,
    safe_cast(
        people_can_cure_themselves_when_suffer as string
    ) people_can_cure_themselves_when_suffer,
    safe_cast(
        people_rely_too_much_doctors_rather as string
    ) people_rely_too_much_doctors_rather,
    safe_cast(
        when_people_sure_medicine_need_doctor as string
    ) when_people_sure_medicine_need_doctor,
    safe_cast(it_best_follow_doctors_orders as string) it_best_follow_doctors_orders,
    safe_cast(
        generally_feel_disappointed_when_leave_doctor as string
    ) generally_feel_disappointed_when_leave_doctor,
    safe_cast(
        doctors_keep_truth_from_patients as string
    ) doctors_keep_truth_from_patients,
    safe_cast(
        regular_general_practitioner_doctor_treat_patients as string
    ) regular_general_practitioner_doctor_treat_patients,
    safe_cast(
        doctors_discuss_treatment_patient_before_they as string
    ) doctors_discuss_treatment_patient_before_they,
    safe_cast(
        patients_reluctant_ask_doctor_questions_they as string
    ) patients_reluctant_ask_doctor_questions_they,
    safe_cast(
        doctors_willing_admit_mistakes_their_patients as string
    ) doctors_willing_admit_mistakes_their_patients,
    safe_cast(
        doctors_use_words_patients_find_difficult as string
    ) doctors_use_words_patients_find_difficult,
    safe_cast(
        citizens_should_spend_some_free_time as string
    ) citizens_should_spend_some_free_time,
    safe_cast(
        society_better_off_if_everyone_looked as string
    ) society_better_off_if_everyone_looked,
    safe_cast(
        citizens_should_not_cheat_taxes as string
    ) citizens_should_not_cheat_taxes,
    safe_cast(
        trust_plumber_builder_mechanic_other_repairer as string
    ) trust_plumber_builder_mechanic_other_repairer,
    safe_cast(
        trust_financial_companies_bank_insurers_deal as string
    ) trust_financial_companies_bank_insurers_deal,
    safe_cast(
        trust_public_officials_deal_honestly as string
    ) trust_public_officials_deal_honestly,
    safe_cast(
        plumber_builder_mechanic_repairer_overcharged_frequency as string
    ) plumber_builder_mechanic_repairer_overcharged_frequency,
    safe_cast(
        were_sold_food_packed_conceal_worse as string
    ) were_sold_food_packed_conceal_worse,
    safe_cast(
        bank_insurance_company_failed_offer_best as string
    ) bank_insurance_company_failed_offer_best,
    safe_cast(
        were_sold_things_second_hand_proved as string
    ) were_sold_things_second_hand_proved,
    safe_cast(
        public_official_asked_favour_bribe_service as string
    ) public_official_asked_favour_bribe_service,
    safe_cast(worried_treated_dishonestly as string) worried_treated_dishonestly,
    safe_cast(
        someone_paying_cash_without_receipt_avoid as string
    ) someone_paying_cash_without_receipt_avoid,
    safe_cast(
        someone_selling_something_second_hand_conceal as string
    ) someone_selling_something_second_hand_conceal,
    safe_cast(
        someone_making_exaggerated_false_insurance_claim as string
    ) someone_making_exaggerated_false_insurance_claim,
    safe_cast(
        public_official_asking_favour_bribe_return as string
    ) public_official_asking_favour_bribe_return,
    safe_cast(
        if_want_make_money_cant_always_act_honestly as string
    ) if_want_make_money_cant_always_act_honestly,
    safe_cast(should_always_obey_law_even_if as string) should_always_obey_law_even_if,
    safe_cast(
        occasionally_alright_ignore_law_do_what_want as string
    ) occasionally_alright_ignore_law_do_what_want,
    safe_cast(
        businesses_only_interested_profit_not_improve as string
    ) businesses_only_interested_profit_not_improve,
    safe_cast(
        nowadays_large_firms_work_together_order as string
    ) nowadays_large_firms_work_together_order,
    safe_cast(
        nowadays_customer_consumer_better_position_protect as string
    ) nowadays_customer_consumer_better_position_protect,
    safe_cast(
        get_benefits_services_not_entitled_number as string
    ) get_benefits_services_not_entitled_number,
    safe_cast(
        kept_change_from_shop_assistant_waiter as string
    ) kept_change_from_shop_assistant_waiter,
    safe_cast(
        paid_cash_no_receipt_avoid_vat_tax_5_years as string
    ) paid_cash_no_receipt_avoid_vat_tax_5_years,
    safe_cast(
        sold_something_second_hand_concealed_its as string
    ) sold_something_second_hand_concealed_its,
    safe_cast(
        misused_altered_card_document_pretend_eligible as string
    ) misused_altered_card_document_pretend_eligible,
    safe_cast(
        made_exaggeration_false_insurance_claim_5_years as string
    ) made_exaggeration_false_insurance_claim_5_years,
    safe_cast(
        offered_favour_bribe_public_official_service as string
    ) offered_favour_bribe_public_official_service,
    safe_cast(
        falsely_claim_government_social_security_other_5 as string
    ) falsely_claim_government_social_security_other_5,
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
    safe_cast(household_member_17_gender as string) household_member_17_gender,
    safe_cast(household_member_18_gender as string) household_member_18_gender,
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
        household_member_17_year_of_birth as string
    ) household_member_17_year_of_birth,
    safe_cast(
        household_member_18_year_of_birth as string
    ) household_member_18_year_of_birth,
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
    safe_cast(
        household_member_17_relationship as string
    ) household_member_17_relationship,
    safe_cast(
        household_member_18_relationship as string
    ) household_member_18_relationship,
    safe_cast(domicile_type as string) domicile_type,
    safe_cast(
        dwelling_owned_any_household_member as string
    ) dwelling_owned_any_household_member,
    safe_cast(
        number_rooms_household_have_use_not as string
    ) number_rooms_household_have_use_not,
    safe_cast(highest_education_3 as string) highest_education_3,
    safe_cast(highest_education_es_isced as string) highest_education_es_isced,
    safe_cast(highest_education_belgium_4 as string) highest_education_belgium_4,
    safe_cast(
        highest_education_switzerland_4 as string
    ) highest_education_switzerland_4,
    safe_cast(highest_education_czechia_2 as string) highest_education_czechia_2,
    safe_cast(highest_education_germany_11 as string) highest_education_germany_11,
    safe_cast(highest_education_denmark_2 as string) highest_education_denmark_2,
    safe_cast(highest_education_estonia_4 as string) highest_education_estonia_4,
    safe_cast(highest_education_spain_5 as string) highest_education_spain_5,
    safe_cast(highest_education_france_4 as string) highest_education_france_4,
    safe_cast(
        highest_education_united_kingdom_9 as string
    ) highest_education_united_kingdom_9,
    safe_cast(highest_education_greece_4 as string) highest_education_greece_4,
    safe_cast(highest_education_hungary_4 as string) highest_education_hungary_4,
    safe_cast(highest_education_ireland_4 as string) highest_education_ireland_4,
    safe_cast(highest_education_italy_4 as string) highest_education_italy_4,
    safe_cast(highest_education_luxembourg as string) highest_education_luxembourg,
    safe_cast(
        highest_education_netherlands_3 as string
    ) highest_education_netherlands_3,
    safe_cast(highest_education_norway_4 as string) highest_education_norway_4,
    safe_cast(highest_education_poland_6 as string) highest_education_poland_6,
    safe_cast(highest_education_portugal_5 as string) highest_education_portugal_5,
    safe_cast(highest_education_sweden_3 as string) highest_education_sweden_3,
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
    safe_cast(
        interviewer_code_one_more_than_one_doing_7_days as string
    ) interviewer_code_one_more_than_one_doing_7_days,
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
    safe_cast(
        proportion_household_income_respondent_provides as string
    ) proportion_household_income_respondent_provides,
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
    safe_cast(
        interviewer_code_one_more_than_one as string
    ) interviewer_code_one_more_than_one,
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
    safe_cast(
        interviewer_code_married_separated_widowed as string
    ) interviewer_code_married_separated_widowed,
    safe_cast(ever_divorced as string) ever_divorced,
    safe_cast(children_living_home_not as string) children_living_home_not,
    safe_cast(children_living_at_home as string) children_living_at_home,
    safe_cast(mother_still_alive as string) mother_still_alive,
    safe_cast(father_still_alive as string) father_still_alive,
    safe_cast(
        have_felt_cheerful_good_spirits_2_weeks as string
    ) have_felt_cheerful_good_spirits_2_weeks,
    safe_cast(have_felt_calm_relaxed_2_weeks as string) have_felt_calm_relaxed_2_weeks,
    safe_cast(
        have_felt_active_vigorous_2_weeks as string
    ) have_felt_active_vigorous_2_weeks,
    safe_cast(
        have_woken_up_feeling_fresh_rested_2_weeks as string
    ) have_woken_up_feeling_fresh_rested_2_weeks,
    safe_cast(
        daily_life_filled_things_interest_me_2_weeks as string
    ) daily_life_filled_things_interest_me_2_weeks,
    safe_cast(
        women_should_prepared_cut_down_paid as string
    ) women_should_prepared_cut_down_paid,
    safe_cast(
        men_should_take_much_responsibility_women as string
    ) men_should_take_much_responsibility_women,
    safe_cast(men_should_have_more_right_job as string) men_should_have_more_right_job,
    safe_cast(
        children_home_parents_should_stay_together as string
    ) children_home_parents_should_stay_together,
    safe_cast(
        person_family_should_main_priority_life as string
    ) person_family_should_main_priority_life,
    safe_cast(
        interviewer_code_lives_husband_wife_partner_4 as string
    ) interviewer_code_lives_husband_wife_partner_4,
    safe_cast(
        year_first_start_living_same_household_partner as string
    ) year_first_start_living_same_household_partner,
    safe_cast(
        frequency_disagree_partner_divide_housework as string
    ) frequency_disagree_partner_divide_housework,
    safe_cast(
        frequency_disagree_partner_money as string
    ) frequency_disagree_partner_money,
    safe_cast(
        frequency_disagree_partner_time_spent_paid_work as string
    ) frequency_disagree_partner_time_spent_paid_work,
    safe_cast(
        who_generally_get_their_way_occasional as string
    ) who_generally_get_their_way_occasional,
    safe_cast(
        who_generally_get_their_way_divide_housework as string
    ) who_generally_get_their_way_divide_housework,
    safe_cast(
        interviewer_code_respondent_partner_working as string
    ) interviewer_code_respondent_partner_working,
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
        total_time_people_home_spend_housework as string
    ) total_time_people_home_spend_housework,
    safe_cast(
        part_spend_total_time_housework_typical as string
    ) part_spend_total_time_housework_typical,
    safe_cast(
        part_partner_spend_total_time_housework as string
    ) part_partner_spend_total_time_housework,
    safe_cast(
        total_time_people_home_spend_housework_2 as string
    ) total_time_people_home_spend_housework_2,
    safe_cast(
        part_spend_total_time_housework_typical_2 as string
    ) part_spend_total_time_housework_typical_2,
    safe_cast(
        part_partner_spend_total_time_housework_2 as string
    ) part_partner_spend_total_time_housework_2,
    safe_cast(
        interviewer_code_respondent_does_no_nearly as string
    ) interviewer_code_respondent_does_no_nearly,
    safe_cast(
        total_time_people_home_spend_housework_3 as string
    ) total_time_people_home_spend_housework_3,
    safe_cast(
        part_spend_total_time_housework_typical_3 as string
    ) part_spend_total_time_housework_typical_3,
    safe_cast(
        total_time_people_home_spend_housework_4 as string
    ) total_time_people_home_spend_housework_4,
    safe_cast(
        part_spend_total_time_housework_typical_4 as string
    ) part_spend_total_time_housework_typical_4,
    safe_cast(
        interviewer_code_respondent_does_no_nearly_2 as string
    ) interviewer_code_respondent_does_no_nearly_2,
    safe_cast(many_things_do_home_often_run as string) many_things_do_home_often_run,
    safe_cast(find_my_housework_monotonous as string) find_my_housework_monotonous,
    safe_cast(
        can_choose_myself_when_do_housework as string
    ) can_choose_myself_when_do_housework,
    safe_cast(find_my_housework_stressful as string) find_my_housework_stressful,
    safe_cast(well_home_equipped_housework as int64) well_home_equipped_housework,
    safe_cast(
        look_after_others_household_children_ill as string
    ) look_after_others_household_children_ill,
    safe_cast(
        give_unpaid_house_care_help_relative as string
    ) give_unpaid_house_care_help_relative,
    safe_cast(
        can_count_unpaid_house_care_help as string
    ) can_count_unpaid_house_care_help,
    safe_cast(
        own_children_adopted_foster_partner_aged as string
    ) own_children_adopted_foster_partner_aged,
    safe_cast(
        care_youngest_child_household_other_than as string
    ) care_youngest_child_household_other_than,
    safe_cast(
        would_ideally_like_more_less_childcare as string
    ) would_ideally_like_more_less_childcare,
    safe_cast(
        children_adopted_foster_partner_any_age as string
    ) children_adopted_foster_partner_any_age,
    safe_cast(
        number_children_not_living_household as string
    ) number_children_not_living_household,
    safe_cast(
        interviewer_code_respondent_has_children_not as string
    ) interviewer_code_respondent_has_children_not,
    safe_cast(
        child_not_living_household_son_daughter as string
    ) child_not_living_household_son_daughter,
    safe_cast(
        year_birth_child_not_living_household as string
    ) year_birth_child_not_living_household,
    safe_cast(
        year_birth_oldest_child_not_living_household as string
    ) year_birth_oldest_child_not_living_household,
    safe_cast(
        year_birth_youngest_child_not_living_household as string
    ) year_birth_youngest_child_not_living_household,
    safe_cast(
        number_daughters_not_living_household as string
    ) number_daughters_not_living_household,
    safe_cast(
        financial_support_children_not_living_household as string
    ) financial_support_children_not_living_household,
    safe_cast(
        everyday_housework_care_support_grown_up as string
    ) everyday_housework_care_support_grown_up,
    safe_cast(
        financial_support_receive_from_grown_up as string
    ) financial_support_receive_from_grown_up,
    safe_cast(
        everyday_housework_care_receive_from_grown as string
    ) everyday_housework_care_receive_from_grown,
    safe_cast(
        interviewer_code_respondent_45_years_younger as string
    ) interviewer_code_respondent_45_years_younger,
    safe_cast(
        plan_having_child_within_next_3_years as string
    ) plan_having_child_within_next_3_years,
    safe_cast(interviewer_code_main_activity as string) interviewer_code_main_activity,
    safe_cast(interviewer_code_employee as string) interviewer_code_employee,
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
    safe_cast(current_job_job_secure as string) current_job_job_secure,
    safe_cast(
        current_job_wage_salary_depends_effort as string
    ) current_job_wage_salary_depends_effort,
    safe_cast(
        current_job_can_get_support_help as string
    ) current_job_can_get_support_help,
    safe_cast(
        current_job_can_decide_time_start_2 as string
    ) current_job_can_decide_time_start_2,
    safe_cast(
        current_job_health_safety_risk_because as string
    ) current_job_health_safety_risk_because,
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
        number_people_immediate_supervisor_boss_responsible as string
    ) number_people_immediate_supervisor_boss_responsible,
    safe_cast(
        immediate_supervisor_boss_man_woman as string
    ) immediate_supervisor_boss_man_woman,
    safe_cast(proportion_women_workplace as string) proportion_women_workplace,
    safe_cast(
        year_first_started_working_current_employer as string
    ) year_first_started_working_current_employer,
    safe_cast(would_turn_down_job_higher_pay as string) would_turn_down_job_higher_pay,
    safe_cast(my_work_closely_supervised as string) my_work_closely_supervised,
    safe_cast(
        get_similar_better_job_another_employer as int64
    ) get_similar_better_job_another_employer,
    safe_cast(
        difficult_easy_employer_replace_if_left as int64
    ) difficult_easy_employer_replace_if_left,
    safe_cast(
        time_usually_take_get_work_minutes as string
    ) time_usually_take_get_work_minutes,
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
        worry_work_problems_when_not_working_frequency as string
    ) worry_work_problems_when_not_working_frequency,
    safe_cast(
        too_tired_after_work_enjoy_things as string
    ) too_tired_after_work_enjoy_things,
    safe_cast(
        job_prevents_from_giving_time_partner as string
    ) job_prevents_from_giving_time_partner,
    safe_cast(
        interviewer_code_dont_have_partner_family_2 as string
    ) interviewer_code_dont_have_partner_family_2,
    safe_cast(
        partner_family_fed_up_pressure_job_frequency_2 as string
    ) partner_family_fed_up_pressure_job_frequency_2,
    safe_cast(
        difficult_concentrate_work_because_family_responsibilities_2 as string
    ) difficult_concentrate_work_because_family_responsibilities_2,
    safe_cast(interviewer_code_employee_2 as string) interviewer_code_employee_2,
    safe_cast(
        usual_gross_pay_euro_before_deductions_2 as string
    ) usual_gross_pay_euro_before_deductions_2,
    safe_cast(
        usual_net_pay_euro_after_deduction_tax_insurance as string
    ) usual_net_pay_euro_after_deduction_tax_insurance,
    safe_cast(long_period_pay_cover_2 as string) long_period_pay_cover_2,
    safe_cast(level_studying as string) level_studying,
    safe_cast(study_place_premises_pleasant as string) study_place_premises_pleasant,
    safe_cast(
        study_place_usually_peace_quiet_during as string
    ) study_place_usually_peace_quiet_during,
    safe_cast(
        study_place_there_teachers_who_treat as string
    ) study_place_there_teachers_who_treat,
    safe_cast(
        study_place_there_students_who_treat as string
    ) study_place_there_students_who_treat,
    safe_cast(
        study_place_teachers_interested_students as string
    ) study_place_teachers_interested_students,
    safe_cast(
        study_place_when_criticise_something_teachers as string
    ) study_place_when_criticise_something_teachers,
    safe_cast(
        study_place_there_students_class_course as string
    ) study_place_there_students_class_course,
    safe_cast(
        studies_prevent_from_time_family_would_like as string
    ) studies_prevent_from_time_family_would_like,
    safe_cast(
        family_responsibilities_prevent_from_time_studies as string
    ) family_responsibilities_prevent_from_time_studies,
    safe_cast(
        get_help_need_from_teachers_course as string
    ) get_help_need_from_teachers_course,
    safe_cast(
        hours_spend_studying_number_average_term as string
    ) hours_spend_studying_number_average_term,
    safe_cast(feel_have_much_do_studies as string) feel_have_much_do_studies,
    safe_cast(find_pace_course_slow_fast as string) find_pace_course_slow_fast,
    safe_cast(year_retirement as string) year_retirement,
    safe_cast(
        wanted_retire_preferred_continue_paid_work as string
    ) wanted_retire_preferred_continue_paid_work,
    safe_cast(
        interviewer_code_respondent_under_70_years_2 as string
    ) interviewer_code_respondent_under_70_years_2,
    safe_cast(
        important_if_choosing_secure_job as string
    ) important_if_choosing_secure_job,
    safe_cast(
        important_if_choosing_high_income as string
    ) important_if_choosing_high_income,
    safe_cast(
        important_if_choosing_good_promotion_opportunities as string
    ) important_if_choosing_good_promotion_opportunities,
    safe_cast(
        important_if_choosing_job_enabled_use_own as string
    ) important_if_choosing_job_enabled_use_own,
    safe_cast(
        important_if_choosing_job_allowed_combine_work as string
    ) important_if_choosing_job_allowed_combine_work,
    safe_cast(
        number_hours_would_choose_work_weekly as string
    ) number_hours_would_choose_work_weekly,
    safe_cast(year_started_working_first_job as string) year_started_working_first_job,
    safe_cast(
        total_number_years_full_part_time_work_2 as string
    ) total_number_years_full_part_time_work_2,
    safe_cast(interviewer_code_gender as string) interviewer_code_gender,
    safe_cast(
        interviewer_code_has_son_daughter as string
    ) interviewer_code_has_son_daughter,
    safe_cast(
        total_time_spent_full_time_home_caring_children as string
    ) total_time_spent_full_time_home_caring_children,
    safe_cast(
        full_time_home_caring_children_negative as string
    ) full_time_home_caring_children_negative,
    safe_cast(
        total_time_part_time_work_rather as string
    ) total_time_part_time_work_rather,
    safe_cast(
        part_time_work_rather_than_full as string
    ) part_time_work_rather_than_full,
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
    safe_cast(region_switzerland as string) region_switzerland,
    safe_cast(region_czechia_2 as string) region_czechia_2,
    safe_cast(region_germany as string) region_germany,
    safe_cast(region_denmark_2 as string) region_denmark_2,
    safe_cast(region_estonia as string) region_estonia,
    safe_cast(region_spain_2 as string) region_spain_2,
    safe_cast(region_finland as string) region_finland,
    safe_cast(region_france as string) region_france,
    safe_cast(region_united_kingdom as string) region_united_kingdom,
    safe_cast(region_greece_2 as string) region_greece_2,
    safe_cast(region_hungary as string) region_hungary,
    safe_cast(region_ireland_3 as string) region_ireland_3,
    safe_cast(region_iceland as string) region_iceland,
    safe_cast(region_italy as string) region_italy,
    safe_cast(region_luxembourg as string) region_luxembourg,
    safe_cast(region_netherlands as string) region_netherlands,
    safe_cast(region_norway as string) region_norway,
    safe_cast(region_poland as string) region_poland,
    safe_cast(region_portugal_2 as string) region_portugal_2,
    safe_cast(region_sweden as string) region_sweden,
    safe_cast(region_slovenia as string) region_slovenia,
    safe_cast(region_slovakia as string) region_slovakia,
    safe_cast(region_turkey as string) region_turkey,
    safe_cast(region_ukraine as string) region_ukraine,
    safe_cast(
        place_interview_east_west_germany as string
    ) place_interview_east_west_germany,
    safe_cast(day_month_interview as string) day_month_interview,
    safe_cast(month_interview as string) month_interview,
    safe_cast(year_interview as string) year_interview,
    safe_cast(start_interview_hour as string) start_interview_hour,
    safe_cast(start_interview_minute as string) start_interview_minute,
    safe_cast(end_interview_hour as string) end_interview_hour,
    safe_cast(end_interview_minute as string) end_interview_minute,
    safe_cast(
        interview_length_minutes_main_questionnaire as int64
    ) interview_length_minutes_main_questionnaire,
    safe_cast(
        administration_split_ballot_mtmm_6 as string
    ) administration_split_ballot_mtmm_6,
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
from {{ set_datalake_project("eu_ess_staging.round_02") }} as t
