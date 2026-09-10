{{
    config(
        schema="us_osha_enforcement",
        alias="violation",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1970, "end": 2035, "interval": 1},
        },
        cluster_by=["inspection_id", "violation_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(inspection_id as string) inspection_id,
    safe_cast(citation_id as string) citation_id,
    safe_cast(standard as string) standard,
    safe_cast(violation_type as string) violation_type,
    safe_cast(deleted as string) deleted,
    safe_cast(issuance_date as date) issuance_date,
    safe_cast(initial_penalty as float64) initial_penalty,
    safe_cast(current_penalty as float64) current_penalty,
    safe_cast(abatement_due_date as date) abatement_due_date,
    safe_cast(abatement_completion_code as string) abatement_completion_code,
    safe_cast(contest_date as date) contest_date,
    safe_cast(final_order_date as date) final_order_date,
    safe_cast(instances as int64) instances,
    safe_cast(employees_exposed as int64) employees_exposed,
    safe_cast(gravity as string) gravity,
    safe_cast(related_event_code as string) related_event_code,
    safe_cast(emphasis_program as string) emphasis_program,
    safe_cast(hazard_category as string) hazard_category,
    safe_cast(hazardous_substance_1 as string) hazardous_substance_1,
    safe_cast(hazardous_substance_2 as string) hazardous_substance_2,
    safe_cast(hazardous_substance_3 as string) hazardous_substance_3,
    safe_cast(hazardous_substance_4 as string) hazardous_substance_4,
    safe_cast(hazardous_substance_5 as string) hazardous_substance_5,
    safe_cast(fta_inspection_id as string) fta_inspection_id,
    safe_cast(fta_issuance_date as date) fta_issuance_date,
    safe_cast(fta_penalty as float64) fta_penalty,
    safe_cast(fta_contest_date as date) fta_contest_date,
    safe_cast(fta_final_order_date as date) fta_final_order_date
from {{ set_datalake_project("us_osha_enforcement_staging.violation") }} as t
