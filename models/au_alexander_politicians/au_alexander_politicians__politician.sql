{{
    config(
        alias="politician",
        schema="au_alexander_politicians",
        materialized="table",
    )
}}
select
    safe_cast(id_politician as string) id_politician,
    safe_cast(id_wikidata as string) id_wikidata,
    safe_cast(id_aph as string) id_aph,
    safe_cast(surname as string) surname,
    safe_cast(all_other_names as string) all_other_names,
    safe_cast(first_name as string) first_name,
    safe_cast(common_name as string) common_name,
    safe_cast(display_name as string) display_name,
    safe_cast(earlier_or_later_names as string) earlier_or_later_names,
    safe_cast(title as string) title,
    safe_cast(gender as string) gender,
    safe_cast(birth_date as date) birth_date,
    safe_cast(birth_year as int64) birth_year,
    safe_cast(birth_place as string) birth_place,
    safe_cast(death_date as date) death_date,
    safe_cast(indicator_member as string) indicator_member,
    safe_cast(indicator_senator as string) indicator_senator,
    safe_cast(indicator_prime_minister as string) indicator_prime_minister,
    safe_cast(url_wikipedia as string) url_wikipedia,
    safe_cast(url_adb as string) url_adb,
    safe_cast(comments as string) comments
from {{ set_datalake_project("au_alexander_politicians_staging.politician") }} as t
