{{
    config(
        schema="au_ato_taxation_statistics",
        alias="individuals_income_state",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2028, "interval": 1},
        },
    )
}}


select
    safe_cast(t.year as int64) year,
    safe_cast(t.state_abbreviation as string) state_abbreviation,
    safe_cast(t.sex as string) sex,
    safe_cast(t.taxable_status as string) taxable_status,
    safe_cast(t.taxable_income_range_code as string) taxable_income_range_code,
    safe_cast(t.taxable_income_range as string) taxable_income_range,
    safe_cast(t.taxable_income_bracket_code as string) taxable_income_bracket_code,
    safe_cast(t.taxable_income_bracket as string) taxable_income_bracket,
    safe_cast(t.item as string) item,
    safe_cast(t.record_count as int64) record_count,
    safe_cast(t.amount as float64) amount
from
    {{
        set_datalake_project(
            "au_ato_taxation_statistics_staging.individuals_income_state"
        )
    }} as t
