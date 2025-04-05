{{ config(alias="country", schema="world_olympedia_olympics") }}
select safe_cast(noc as string) noc, safe_cast(country as string) name,
from {{ project_path("world_olympedia_olympics_staging.country") }} as t
