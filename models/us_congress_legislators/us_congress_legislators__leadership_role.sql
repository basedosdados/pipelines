{{
    config(
        alias="leadership_role",
        schema="us_congress_legislators",
        materialized="table",
    )
}}
-- Source snapshot: unitedstates/congress-legislators@d5af3d2 (2026-09-24)
select
    safe_cast(id_bioguide as string) id_bioguide,
    safe_cast(title as string) title,
    safe_cast(chamber as string) chamber,
    safe_cast(role_start as date) role_start,
    safe_cast(role_end as date) role_end
from {{ set_datalake_project("us_congress_legislators_staging.leadership_role") }} as t
