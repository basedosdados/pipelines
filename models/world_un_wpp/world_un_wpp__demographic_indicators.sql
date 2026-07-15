{{
    config(
        schema="world_un_wpp",
        alias="demographic_indicators",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1950, "end": 2106, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(total_population_1_january as float64) total_population_1_january,
    safe_cast(total_population_1_july as float64) total_population_1_july,
    safe_cast(total_population_male_1_july as float64) total_population_male_1_july,
    safe_cast(total_population_female_1_july as float64) total_population_female_1_july,
    safe_cast(population_density as float64) population_density,
    safe_cast(population_sex_ratio as float64) population_sex_ratio,
    safe_cast(median_age as float64) median_age,
    safe_cast(natural_change as float64) natural_change,
    safe_cast(rate_natural_change as float64) rate_natural_change,
    safe_cast(population_change as float64) population_change,
    safe_cast(population_growth_rate as float64) population_growth_rate,
    safe_cast(population_doubling_time as float64) population_doubling_time,
    safe_cast(births as float64) births,
    safe_cast(births_women_age_15_19 as float64) births_women_age_15_19,
    safe_cast(crude_birth_rate as float64) crude_birth_rate,
    safe_cast(total_fertility_rate as float64) total_fertility_rate,
    safe_cast(net_reproduction_rate as float64) net_reproduction_rate,
    safe_cast(mean_age_childbearing as float64) mean_age_childbearing,
    safe_cast(sex_ratio_at_birth as float64) sex_ratio_at_birth,
    safe_cast(total_deaths as float64) total_deaths,
    safe_cast(total_deaths_male as float64) total_deaths_male,
    safe_cast(total_deaths_female as float64) total_deaths_female,
    safe_cast(crude_death_rate as float64) crude_death_rate,
    safe_cast(life_expectancy_at_birth as float64) life_expectancy_at_birth,
    safe_cast(life_expectancy_at_birth_male as float64) life_expectancy_at_birth_male,
    safe_cast(
        life_expectancy_at_birth_female as float64
    ) life_expectancy_at_birth_female,
    safe_cast(life_expectancy_at_15 as float64) life_expectancy_at_15,
    safe_cast(life_expectancy_at_15_male as float64) life_expectancy_at_15_male,
    safe_cast(life_expectancy_at_15_female as float64) life_expectancy_at_15_female,
    safe_cast(life_expectancy_at_65 as float64) life_expectancy_at_65,
    safe_cast(life_expectancy_at_65_male as float64) life_expectancy_at_65_male,
    safe_cast(life_expectancy_at_65_female as float64) life_expectancy_at_65_female,
    safe_cast(life_expectancy_at_80 as float64) life_expectancy_at_80,
    safe_cast(life_expectancy_at_80_male as float64) life_expectancy_at_80_male,
    safe_cast(life_expectancy_at_80_female as float64) life_expectancy_at_80_female,
    safe_cast(infant_deaths as float64) infant_deaths,
    safe_cast(infant_mortality_rate as float64) infant_mortality_rate,
    safe_cast(live_births_surviving_age_1 as float64) live_births_surviving_age_1,
    safe_cast(under_5_deaths as float64) under_5_deaths,
    safe_cast(under_5_mortality_rate as float64) under_5_mortality_rate,
    safe_cast(mortality_before_age_40 as float64) mortality_before_age_40,
    safe_cast(mortality_before_age_40_male as float64) mortality_before_age_40_male,
    safe_cast(mortality_before_age_40_female as float64) mortality_before_age_40_female,
    safe_cast(mortality_before_age_60 as float64) mortality_before_age_60,
    safe_cast(mortality_before_age_60_male as float64) mortality_before_age_60_male,
    safe_cast(mortality_before_age_60_female as float64) mortality_before_age_60_female,
    safe_cast(mortality_between_age_15_50 as float64) mortality_between_age_15_50,
    safe_cast(
        mortality_between_age_15_50_male as float64
    ) mortality_between_age_15_50_male,
    safe_cast(
        mortality_between_age_15_50_female as float64
    ) mortality_between_age_15_50_female,
    safe_cast(mortality_between_age_15_60 as float64) mortality_between_age_15_60,
    safe_cast(
        mortality_between_age_15_60_male as float64
    ) mortality_between_age_15_60_male,
    safe_cast(
        mortality_between_age_15_60_female as float64
    ) mortality_between_age_15_60_female,
    safe_cast(net_migrations as float64) net_migrations,
    safe_cast(net_migration_rate as float64) net_migration_rate
from {{ set_datalake_project("world_un_wpp_staging.demographic_indicators") }} as t
