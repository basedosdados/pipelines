{{
    config(
        schema="us_oi_opportunity_atlas",
        alias="commuting_zone_migration",
        materialized="table",
        cluster_by=["origin_commuting_zone_id"],
    )
}}


select
    safe_cast(origin_commuting_zone_id as string) origin_commuting_zone_id,
    safe_cast(origin_commuting_zone_name as string) origin_commuting_zone_name,
    safe_cast(origin_state_name as string) origin_state_name,
    safe_cast(destination_commuting_zone_id as string) destination_commuting_zone_id,
    safe_cast(
        destination_commuting_zone_name as string
    ) destination_commuting_zone_name,
    safe_cast(destination_state_name as string) destination_state_name,
    safe_cast(race as string) race,
    safe_cast(parent_income_quintile as string) parent_income_quintile,
    safe_cast(individuals as int64) individuals,
    safe_cast(origin_individuals as int64) origin_individuals,
    safe_cast(destination_individuals as int64) destination_individuals,
    safe_cast(
        probability_destination_given_origin as float64
    ) probability_destination_given_origin,
    safe_cast(
        probability_origin_given_destination as float64
    ) probability_origin_given_destination
from
    {{
        set_datalake_project(
            "us_oi_opportunity_atlas_staging.commuting_zone_migration"
        )
    }} as t
