{{
    config(
        schema="au_qld_ecq_elections",
        alias="election",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(election_id as string) election_id,
    safe_cast(election_stub as string) election_stub,
    safe_cast(election_name as string) election_name,
    safe_cast(election_type as string) election_type,
    safe_cast(government_level as string) government_level,
    safe_cast(election_date as date) election_date,
    safe_cast(results_archive_url as string) results_archive_url
from {{ set_datalake_project("au_qld_ecq_elections_staging.election") }} as t
