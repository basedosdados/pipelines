{{
    config(
        schema="world_openalex",
        alias="institution_association",
        materialized="table",
        cluster_by=["institution_id"],
    )
}}


select
    safe_cast(institution_id as string) institution_id,
    safe_cast(associated_institution_id as string) associated_institution_id,
    safe_cast(relationship as string) relationship
from {{ set_datalake_project("world_openalex_staging.institution_association") }} as t
