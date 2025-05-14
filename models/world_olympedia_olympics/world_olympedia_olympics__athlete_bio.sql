{{ config(alias="athlete_bio", schema="world_olympedia_olympics") }}
select
    safe_cast(athlete_id as string) athlete_id,
    safe_cast(name as string) name,
    safe_cast(sex as string) sex,
    safe_cast(born as date) birth_date,
    safe_cast(year_born as int64) birth_year,
    safe_cast(height as float64) height,
    safe_cast(weight as float64) weight,
    safe_cast(country as string) country,
    safe_cast(country_noc as string) country_noc,
    safe_cast(description as string) description,
    safe_cast(special_notes as string) special_notes,
from {{ set_datalake_project("world_olympedia_olympics_staging.athlete_bio") }} as t
