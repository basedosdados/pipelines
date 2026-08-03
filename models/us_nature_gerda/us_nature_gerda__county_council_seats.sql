{{
    config(
        schema="us_nature_gerda",
        alias="county_council_seats",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1945, "end": 2031, "interval": 1},
        },
        cluster_by=["id_county", "party"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(id_county as string) id_county,
    safe_cast(id_state as string) id_state,
    safe_cast(county_name as string) county_name,
    safe_cast(county_type as string) county_type,
    safe_cast(government_party as string) government_party,
    safe_cast(seats_total as int64) seats_total,
    safe_cast(seats_regional as int64) seats_regional,
    safe_cast(seats_other as int64) seats_other,
    safe_cast(seats_local_other as int64) seats_local_other,
    safe_cast(flag_seats_total_incongruent as string) flag_seats_total_incongruent,
    safe_cast(party as string) party,
    safe_cast(seats as int64) seats
from {{ set_datalake_project("us_nature_gerda_staging.county_council_seats") }} as t
