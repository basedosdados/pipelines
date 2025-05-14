{{
    config(
        alias="winner_demographics",
        schema="world_ampas_oscar",
        materialized="table",
    )
}}

select distinct
    safe_cast(name as string) name,
    safe_cast(birth_year as int64) birth_year,
    safe_cast(birth_date as date) birth_date,
    safe_cast(birthplace as string) birthplace,
    safe_cast(race_ethnicity as string) race_ethnicity,
    safe_cast(religion as string) religion,
    safe_cast(sexual_orientation as string) sexual_orientation,
    safe_cast(year_edition as int64) year_edition,
    safe_cast(category as string) category,
    safe_cast(movie as string) movie,
from {{ set_datalake_project("world_ampas_oscar_staging.winner_demographics") }} as t
