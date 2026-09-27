{{
    config(
        schema="us_treasury_fiscaldata",
        alias="debt_outstanding",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1993, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(record_date as date) record_date,
    safe_cast(total_public_debt_outstanding as float64) total_public_debt_outstanding,
    safe_cast(debt_held_by_public as float64) debt_held_by_public,
    safe_cast(intragovernmental_holdings as float64) intragovernmental_holdings
from {{ set_datalake_project("us_treasury_fiscaldata_staging.debt_outstanding") }} as t
