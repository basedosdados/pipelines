{{
    config(
        schema="au_qld_ecq_elections",
        alias="disclosure_expenditure",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(date_incurred as date) date_incurred,
    safe_cast(incurred_by_name as string) incurred_by_name,
    safe_cast(expenditure_value as float64) expenditure_value,
    safe_cast(candidate_type as string) candidate_type,
    safe_cast(local_electorate_name as string) local_electorate_name,
    safe_cast(election_name as string) election_name,
    safe_cast(goods_or_services_description as string) goods_or_services_description,
    safe_cast(expenditure_purpose as string) expenditure_purpose
from
    {{ set_datalake_project("au_qld_ecq_elections_staging.disclosure_expenditure") }}
    as t
