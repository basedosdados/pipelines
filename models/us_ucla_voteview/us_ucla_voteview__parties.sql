{{
    config(
        alias="parties",
        schema="us_ucla_voteview",
        materialized="table",
    )
}}
select
    safe_cast(congress as int64) congress,
    safe_cast(chamber as string) chamber,
    safe_cast(party_code as string) party_code,
    safe_cast(party_name as string) party_name,
    safe_cast(n_members as int64) n_members,
    safe_cast(nominate_dim1_median as float64) nominate_dim1_median,
    safe_cast(nominate_dim2_median as float64) nominate_dim2_median,
    safe_cast(nominate_dim1_mean as float64) nominate_dim1_mean,
    safe_cast(nominate_dim2_mean as float64) nominate_dim2_mean
from {{ set_datalake_project("us_ucla_voteview_staging.parties") }} as t
