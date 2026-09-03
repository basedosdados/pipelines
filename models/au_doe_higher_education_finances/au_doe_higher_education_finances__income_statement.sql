{{
    config(
        schema="au_doe_higher_education_finances",
        alias="income_statement",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2031, "interval": 1},
        },
        cluster_by=["hep_code", "line_item"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(hep_code as string) hep_code,
    safe_cast(institution_type as string) institution_type,
    safe_cast(line_number as int64) line_number,
    safe_cast(line_item as string) line_item,
    safe_cast(line_item_internal as string) line_item_internal,
    safe_cast(value as int64) value
from
    {{
        set_datalake_project(
            "au_doe_higher_education_finances_staging.income_statement"
        )
    }} as t
