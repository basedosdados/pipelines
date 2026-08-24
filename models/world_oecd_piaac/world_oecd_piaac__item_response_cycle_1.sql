{{
    config(
        alias="item_response_cycle_1",
        schema="world_oecd_piaac",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2012, "end": 2028, "interval": 1},
        },
        cluster_by=["country_id_iso_3", "item_code"],
    )
}}


select
    safe_cast(year as int64) year,
    nullif(nullif(trim(safe_cast(cycle as string)), ''), '.') cycle,
    nullif(nullif(trim(safe_cast(round as string)), ''), '.') round,
    nullif(
        nullif(trim(safe_cast(country_id_iso_3 as string)), ''), '.'
    ) country_id_iso_3,
    nullif(nullif(trim(safe_cast(country_id_m49 as string)), ''), '.') country_id_m49,
    nullif(
        nullif(trim(safe_cast(country_entity_id as string)), ''), '.'
    ) country_entity_id,
    nullif(nullif(trim(safe_cast(respondent_id as string)), ''), '.') respondent_id,
    nullif(nullif(trim(safe_cast(item_code as string)), ''), '.') item_code,
    nullif(nullif(trim(safe_cast(domain as string)), ''), '.') domain,
    nullif(nullif(trim(safe_cast(scored_response as string)), ''), '.') scored_response,
    nullif(
        nullif(trim(safe_cast(scored_response_label as string)), ''), '.'
    ) scored_response_label,
    nullif(nullif(trim(safe_cast(raw_response as string)), ''), '.') raw_response,
    safe_cast(timing_seconds as float64) timing_seconds,
    safe_cast(timing_first_action_seconds as float64) timing_first_action_seconds,
    safe_cast(n_actions as int64) n_actions,
    safe_cast(n_visits as int64) n_visits,
    safe_cast(n_short_visits as int64) n_short_visits
from {{ set_datalake_project("world_oecd_piaac_staging.item_response_cycle_1") }} as t
