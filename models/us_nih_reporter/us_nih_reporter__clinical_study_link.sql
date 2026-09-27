{{
    config(
        schema="us_nih_reporter",
        alias="clinical_study_link",
        materialized="table",
    )
}}


select
    safe_cast(nct_id as string) nct_id,
    safe_cast(core_project_num as string) core_project_num,
    safe_cast(study_title as string) study_title,
    safe_cast(study_status as string) study_status
from {{ set_datalake_project("us_nih_reporter_staging.clinical_study_link") }} as t
