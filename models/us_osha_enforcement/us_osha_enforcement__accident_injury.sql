{{
    config(
        schema="us_osha_enforcement",
        alias="accident_injury",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1970, "end": 2035, "interval": 1},
        },
        cluster_by=["accident_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(accident_id as string) accident_id,
    safe_cast(inspection_id as string) inspection_id,
    safe_cast(injury_line_number as string) injury_line_number,
    safe_cast(age as int64) age,
    safe_cast(sex as string) sex,
    safe_cast(degree_of_injury as string) degree_of_injury,
    safe_cast(nature_of_injury as string) nature_of_injury,
    safe_cast(part_of_body as string) part_of_body,
    safe_cast(source_of_injury as string) source_of_injury,
    safe_cast(event_type as string) event_type,
    safe_cast(environmental_factor as string) environmental_factor,
    safe_cast(human_factor as string) human_factor,
    safe_cast(occupation_code as string) occupation_code,
    safe_cast(task_assigned as string) task_assigned,
    safe_cast(hazardous_substance as string) hazardous_substance,
    safe_cast(construction_operation as string) construction_operation,
    safe_cast(construction_operation_cause as string) construction_operation_cause,
    safe_cast(fatality_cause as string) fatality_cause,
    safe_cast(fall_distance as int64) fall_distance,
    safe_cast(fall_height as int64) fall_height
from {{ set_datalake_project("us_osha_enforcement_staging.accident_injury") }} as t
