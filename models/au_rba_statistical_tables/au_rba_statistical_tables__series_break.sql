{{
    config(
        schema="au_rba_statistical_tables",
        alias="series_break",
        materialized="table",
    )
}}


select
    safe_cast(table_code as string) table_code,
    safe_cast(table_name as string) table_name,
    safe_cast(date as date) date,
    safe_cast(break_type as string) break_type,
    safe_cast(series_title as string) series_title,
    safe_cast(details as string) details
from {{ set_datalake_project("au_rba_statistical_tables_staging.series_break") }} as t
