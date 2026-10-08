{{
    config(
        alias="members",
        schema="us_ucla_voteview",
        materialized="table",
    )
}}
select
    safe_cast(congress as int64) congress,
    safe_cast(chamber as string) chamber,
    safe_cast(icpsr as string) icpsr,
    safe_cast(state_icpsr as string) state_icpsr,
    safe_cast(district_code as string) district_code,
    safe_cast(state_abbrev as string) state_abbrev,
    safe_cast(party_code as string) party_code,
    safe_cast(occupancy as string) occupancy,
    safe_cast(last_means as string) last_means,
    safe_cast(bioname as string) bioname,
    safe_cast(bioguide_id as string) bioguide_id,
    safe_cast(born as int64) born,
    safe_cast(died as int64) died,
    safe_cast(nominate_dim1 as float64) nominate_dim1,
    safe_cast(nominate_dim2 as float64) nominate_dim2,
    safe_cast(nominate_log_likelihood as float64) nominate_log_likelihood,
    safe_cast(nominate_geo_mean_probability as float64) nominate_geo_mean_probability,
    safe_cast(nominate_number_of_votes as int64) nominate_number_of_votes,
    safe_cast(nominate_number_of_errors as int64) nominate_number_of_errors,
    safe_cast(conditional as string) conditional,
    safe_cast(nokken_poole_dim1 as float64) nokken_poole_dim1,
    safe_cast(nokken_poole_dim2 as float64) nokken_poole_dim2
from {{ set_datalake_project("us_ucla_voteview_staging.members") }} as t
