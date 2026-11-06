{{
    config(
        schema="us_bls_cex",
        alias="series",
        materialized="table",
    )
}}


select
    safe_cast(series_id as string) series_id,
    safe_cast(category_id as string) category_id,
    safe_cast(category_name as string) category_name,
    safe_cast(subcategory_id as string) subcategory_id,
    safe_cast(subcategory_name as string) subcategory_name,
    safe_cast(item_id as string) item_id,
    safe_cast(item_name as string) item_name,
    safe_cast(item_display_level as int64) item_display_level,
    safe_cast(demographics_id as string) demographics_id,
    safe_cast(demographics_name as string) demographics_name,
    safe_cast(characteristics_id as string) characteristics_id,
    safe_cast(characteristics_name as string) characteristics_name,
    safe_cast(statistic as string) statistic,
    safe_cast(series_title as string) series_title,
    safe_cast(begin_year as int64) begin_year,
    safe_cast(end_year as int64) end_year
from {{ set_datalake_project("us_bls_cex_staging.series") }} as t
