{{
    config(
        schema="world_cricsheet",
        alias="people",
        materialized="table",
    )
}}


select
    safe_cast(identifier as string) person_id,
    safe_cast(name as string) name,
    safe_cast(unique_name as string) unique_name,
    safe_cast(key_bcci as string) key_bcci,
    safe_cast(key_bcci_2 as string) key_bcci_2,
    safe_cast(key_bigbash as string) key_bigbash,
    safe_cast(key_cricbuzz as string) key_cricbuzz,
    safe_cast(key_cricheroes as string) key_cricheroes,
    safe_cast(key_crichq as string) key_crichq,
    safe_cast(key_cricinfo as string) key_cricinfo,
    safe_cast(key_cricinfo_2 as string) key_cricinfo_2,
    safe_cast(key_cricinfo_3 as string) key_cricinfo_3,
    safe_cast(key_cricingif as string) key_cricingif,
    safe_cast(key_cricketarchive as string) key_cricketarchive,
    safe_cast(key_cricketarchive_2 as string) key_cricketarchive_2,
    safe_cast(key_cricketworld as string) key_cricketworld,
    safe_cast(key_nvplay as string) key_nvplay,
    safe_cast(key_nvplay_2 as string) key_nvplay_2,
    safe_cast(key_opta as string) key_opta,
    safe_cast(key_opta_2 as string) key_opta_2,
    safe_cast(key_pulse as string) key_pulse,
    safe_cast(key_pulse_2 as string) key_pulse_2
from {{ set_datalake_project("world_cricsheet_staging.people") }} as t
