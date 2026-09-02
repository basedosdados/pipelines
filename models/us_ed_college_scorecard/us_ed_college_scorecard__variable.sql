{{
    config(
        schema="us_ed_college_scorecard",
        alias="variable",
        materialized="table",
    )
}}


select
    safe_cast(variable_name as string) variable_name,
    safe_cast(source_file as string) source_file,
    safe_cast(table_name as string) table_name,
    safe_cast(api_name as string) api_name,
    safe_cast(data_type as string) data_type,
    safe_cast(label as string) label
from
    {{ set_datalake_project("us_ed_college_scorecard_staging.variable") }}
    as t
