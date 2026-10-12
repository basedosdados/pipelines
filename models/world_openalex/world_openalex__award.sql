{{
    config(
        schema="world_openalex",
        alias="award",
        materialized="table",
        cluster_by=["award_id"],
    )
}}


select
    safe_cast(award_id as string) award_id,
    safe_cast(display_name as string) display_name,
    safe_cast(description as string) description,
    safe_cast(funder_id as string) funder_id,
    safe_cast(funder_award_id as string) funder_award_id,
    safe_cast(funding_type as string) funding_type,
    safe_cast(funder_scheme as string) funder_scheme,
    safe_cast(amount as float64) amount,
    safe_cast(currency as string) currency,
    safe_cast(start_date as date) start_date,
    safe_cast(end_date as date) end_date,
    safe_cast(start_year as int64) start_year,
    safe_cast(end_year as int64) end_year,
    safe_cast(primary_topic_id as string) primary_topic_id,
    safe_cast(funded_outputs_count as int64) funded_outputs_count,
    safe_cast(doi as string) doi,
    safe_cast(landing_page_url as string) landing_page_url,
    safe_cast(provenance as string) provenance,
    safe_cast(created_date as date) created_date,
    safe_cast(updated_date as date) updated_date
from {{ set_datalake_project("world_openalex_staging.award") }} as t
