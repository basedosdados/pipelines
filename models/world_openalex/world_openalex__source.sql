{{
    config(
        schema="world_openalex",
        alias="source",
        materialized="table",
        cluster_by=["source_id"],
    )
}}


select
    safe_cast(source_id as string) source_id,
    safe_cast(issn_l as string) issn_l,
    safe_cast(display_name as string) display_name,
    safe_cast(type as string) type,
    safe_cast(host_organization_id as string) host_organization_id,
    safe_cast(country_code as string) country_code,
    safe_cast(homepage_url as string) homepage_url,
    safe_cast(is_open_access as bool) is_open_access,
    safe_cast(is_in_doaj as bool) is_in_doaj,
    safe_cast(is_in_doaj_since_year as int64) is_in_doaj_since_year,
    safe_cast(is_in_scielo as bool) is_in_scielo,
    safe_cast(is_ojs as bool) is_ojs,
    safe_cast(is_core as bool) is_core,
    safe_cast(is_preprint_repository as bool) is_preprint_repository,
    safe_cast(oa_flip_year as int64) oa_flip_year,
    safe_cast(first_publication_year as int64) first_publication_year,
    safe_cast(last_publication_year as int64) last_publication_year,
    safe_cast(apc_usd as int64) apc_usd,
    safe_cast(works_count as int64) works_count,
    safe_cast(oa_works_count as int64) oa_works_count,
    safe_cast(cited_by_count as int64) cited_by_count,
    safe_cast(two_year_mean_citedness as float64) two_year_mean_citedness,
    safe_cast(h_index as int64) h_index,
    safe_cast(i10_index as int64) i10_index,
    safe_cast(created_date as date) created_date,
    safe_cast(updated_date as date) updated_date
from {{ set_datalake_project("world_openalex_staging.source") }} as t
