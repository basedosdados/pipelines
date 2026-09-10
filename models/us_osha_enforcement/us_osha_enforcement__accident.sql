{{
    config(
        schema="us_osha_enforcement",
        alias="accident",
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
    safe_cast(reporting_office_id as string) reporting_office_id,
    safe_cast(event_date as date) event_date,
    safe_cast(event_description as string) event_description,
    safe_cast(event_keyword as string) event_keyword,
    safe_cast(fatality as string) fatality,
    safe_cast(sic_list as string) sic_list,
    safe_cast(construction_end_use as string) construction_end_use,
    safe_cast(project_type as string) project_type,
    safe_cast(project_cost as string) project_cost,
    safe_cast(building_stories as int64) building_stories,
    safe_cast(nonbuilding_height as int64) nonbuilding_height
from {{ set_datalake_project("us_osha_enforcement_staging.accident") }} as t
