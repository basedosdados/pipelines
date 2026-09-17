{{
    config(
        schema="au_abs_national_accounts",
        alias="series",
        materialized="table",
    )
}}


select
    safe_cast(series_id as string) series_id,
    safe_cast(description as string) description,
    safe_cast(unit as string) unit,
    safe_cast(source_table_no as string) source_table_no,
    safe_cast(source_table_name as string) source_table_name,
    safe_cast(series_start as date) series_start,
    safe_cast(series_end as date) series_end
from {{ set_datalake_project("au_abs_national_accounts_staging.series") }} as t
