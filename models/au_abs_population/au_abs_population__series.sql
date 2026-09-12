{{
    config(
        schema="au_abs_population",
        alias="series",
        materialized="table",
    )
}}


select
    safe_cast(series_id as string) series_id,
    safe_cast(description as string) description,
    safe_cast(unit as string) unit,
    safe_cast(frequency as string) frequency,
    safe_cast(source_catalogue as string) source_catalogue,
    safe_cast(source_table as string) source_table,
    safe_cast(series_start as date) series_start,
    safe_cast(series_end as date) series_end
from {{ set_datalake_project("au_abs_population_staging.series") }} as t
