{{
    config(
        schema="us_nih_reporter",
        alias="patent_link",
        materialized="table",
    )
}}


select
    safe_cast(patent_id as string) patent_id,
    safe_cast(core_project_num as string) core_project_num,
    safe_cast(patent_title as string) patent_title,
    safe_cast(patent_org_name as string) patent_org_name
from {{ set_datalake_project("us_nih_reporter_staging.patent_link") }} as t
