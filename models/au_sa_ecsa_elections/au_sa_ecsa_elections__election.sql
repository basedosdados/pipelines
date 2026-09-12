{{
    config(
        schema="au_sa_ecsa_elections",
        alias="election",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(election_id as string) election_id,
    safe_cast(election_name as string) election_name,
    safe_cast(election_type as string) election_type,
    safe_cast(government_level as string) government_level,
    safe_cast(election_date as date) election_date,
    safe_cast(assembly_districts_contested as int64) assembly_districts_contested,
    safe_cast(council_seats_contested as int64) council_seats_contested,
    safe_cast(results_last_updated as datetime) results_last_updated,
    safe_cast(results_data_version as string) results_data_version,
    safe_cast(results_source_url as string) results_source_url
from {{ set_datalake_project("au_sa_ecsa_elections_staging.election") }} as t
