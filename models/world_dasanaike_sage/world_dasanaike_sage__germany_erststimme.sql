-- Reads staging rows built by models/world_dasanaike_sage/code/ (download.py ->
-- clean.py -> upload.py).
{{
    config(
        schema="world_dasanaike_sage",
        alias="germany_erststimme",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2026, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(wahlkreis_nr as string) wahlkreis_nr,
    safe_cast(wahlkreis_name as string) wahlkreis_name,
    safe_cast(party as string) party,
    safe_cast(candidate as string) candidate,
    safe_cast(constituency_votes as int64) constituency_votes,
    safe_cast(constituency_share_pct as float64) constituency_share_pct
from {{ set_datalake_project("world_dasanaike_sage_staging.germany_erststimme") }} as t
