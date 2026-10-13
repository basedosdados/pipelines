{{
    config(
        schema="us_cms_hcris",
        alias="report_value",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2031, "interval": 1},
        },
        cluster_by=["worksheet_code", "line_number", "column_number"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(report_id as string) report_id,
    safe_cast(provider_ccn as string) provider_ccn,
    safe_cast(form_version as string) form_version,
    safe_cast(worksheet_code as string) worksheet_code,
    safe_cast(line_number as string) line_number,
    safe_cast(column_number as string) column_number,
    safe_cast(numeric_value as float64) numeric_value,
    safe_cast(alpha_value as string) alpha_value
from {{ set_datalake_project("us_cms_hcris_staging.report_value") }} as t
