{{
    config(
        schema="us_sec_edgar",
        alias="tag",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2031, "interval": 1},
        },
        cluster_by=["tag"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(tag as string) tag,
    safe_cast(version as string) version,
    safe_cast(custom as string) custom,
    safe_cast(abstract as string) abstract,
    safe_cast(datatype as string) datatype,
    safe_cast(period_type as string) period_type,
    safe_cast(balance as string) balance,
    safe_cast(label as string) label,
    safe_cast(documentation as string) documentation
from {{ set_datalake_project("us_sec_edgar_staging.tag") }} as t
