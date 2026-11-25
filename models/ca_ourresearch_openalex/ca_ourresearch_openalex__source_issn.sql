{{
    config(
        schema="ca_ourresearch_openalex",
        alias="source_issn",
        materialized="table",
        cluster_by=["source_id"],
    )
}}


select safe_cast(source_id as string) source_id, safe_cast(issn as string) issn
from {{ set_datalake_project("ca_ourresearch_openalex_staging.source_issn") }} as t
