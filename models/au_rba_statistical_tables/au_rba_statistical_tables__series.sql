{{
    config(
        schema="au_rba_statistical_tables",
        alias="series",
        materialized="table",
    )
}}


select
    safe_cast(table_code as string) table_code,
    safe_cast(series_id as string) series_id,
    safe_cast(table_name as string) table_name,
    safe_cast(title as string) title,
    safe_cast(description as string) description,
    safe_cast(frequency as string) frequency,
    safe_cast(series_type as string) series_type,
    safe_cast(units as string) units,
    safe_cast(source as string) source,
    safe_cast(publication_date as date) publication_date,
    safe_cast(observation_start as date) observation_start,
    safe_cast(observation_end as date) observation_end
from {{ set_datalake_project("au_rba_statistical_tables_staging.series") }} as t
