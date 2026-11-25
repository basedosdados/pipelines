{{
    config(
        schema="us_bls_cex",
        alias="ucc",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1996, "end": 2031, "interval": 1},
        },
        cluster_by=["hierarchy"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(hierarchy as string) hierarchy,
    safe_cast(line_number as int64) line_number,
    safe_cast(level as int64) level,
    safe_cast(title as string) title,
    safe_cast(ucc as string) ucc,
    safe_cast(row_type as string) row_type,
    safe_cast(factor as string) factor,
    safe_cast(section as string) section,
    safe_cast(parent_ucc as string) parent_ucc
from {{ set_datalake_project("us_bls_cex_staging.ucc") }} as t
