{{
    config(
        schema="us_treasury_fiscaldata",
        alias="mts_means_of_financing",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(record_date as date) record_date,
    safe_cast(fiscal_year as int64) fiscal_year,
    safe_cast(src_line_nbr as string) src_line_nbr,
    safe_cast(parent_id as string) parent_id,
    safe_cast(classification_id as string) classification_id,
    safe_cast(classification_desc as string) classification_desc,
    safe_cast(sequence_level_nbr as string) sequence_level_nbr,
    safe_cast(current_month_net_transactions as float64) current_month_net_transactions,
    safe_cast(current_fytd_net_transactions as float64) current_fytd_net_transactions,
    safe_cast(prior_fytd_net_transactions as float64) prior_fytd_net_transactions,
    safe_cast(beginning_year_balance as float64) beginning_year_balance,
    safe_cast(beginning_month_balance as float64) beginning_month_balance,
    safe_cast(closing_month_balance as float64) closing_month_balance
from
    {{ set_datalake_project("us_treasury_fiscaldata_staging.mts_means_of_financing") }}
    as t
