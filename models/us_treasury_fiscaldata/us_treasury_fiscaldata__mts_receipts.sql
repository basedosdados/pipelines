{{
    config(
        schema="us_treasury_fiscaldata",
        alias="mts_receipts",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2031, "interval": 1},
        },
        cluster_by=["classification_desc"],
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
    safe_cast(current_month_gross_receipts as float64) current_month_gross_receipts,
    safe_cast(current_month_refunds as float64) current_month_refunds,
    safe_cast(current_month_net_receipts as float64) current_month_net_receipts,
    safe_cast(current_fytd_gross_receipts as float64) current_fytd_gross_receipts,
    safe_cast(current_fytd_refunds as float64) current_fytd_refunds,
    safe_cast(current_fytd_net_receipts as float64) current_fytd_net_receipts,
    safe_cast(prior_fytd_gross_receipts as float64) prior_fytd_gross_receipts,
    safe_cast(prior_fytd_refunds as float64) prior_fytd_refunds,
    safe_cast(prior_fytd_net_receipts as float64) prior_fytd_net_receipts
from {{ set_datalake_project("us_treasury_fiscaldata_staging.mts_receipts") }} as t
