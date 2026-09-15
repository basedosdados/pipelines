{{
    config(
        schema="us_treasury_fiscaldata",
        alias="average_interest_rate",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2001, "end": 2031, "interval": 1},
        },
        cluster_by=["security_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(record_date as date) record_date,
    safe_cast(fiscal_year as int64) fiscal_year,
    safe_cast(security_type as string) security_type,
    safe_cast(security_desc as string) security_desc,
    safe_cast(avg_interest_rate as float64) avg_interest_rate
from
    {{ set_datalake_project("us_treasury_fiscaldata_staging.average_interest_rate") }}
    as t
