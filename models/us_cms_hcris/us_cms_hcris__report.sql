{{
    config(
        schema="us_cms_hcris",
        alias="report",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2031, "interval": 1},
        },
        cluster_by=["provider_ccn"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(report_id as string) report_id,
    safe_cast(provider_ccn as string) provider_ccn,
    safe_cast(state_id as string) state_id,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(npi as string) npi,
    safe_cast(form_version as string) form_version,
    safe_cast(source_extract_year as int64) source_extract_year,
    safe_cast(provider_control_type_code as string) provider_control_type_code,
    safe_cast(report_status_code as string) report_status_code,
    safe_cast(fiscal_year_begin_date as date) fiscal_year_begin_date,
    safe_cast(fiscal_year_end_date as date) fiscal_year_end_date,
    safe_cast(fiscal_year_days as int64) fiscal_year_days,
    safe_cast(process_date as date) process_date,
    safe_cast(initial_report_indicator as string) initial_report_indicator,
    safe_cast(last_report_indicator as string) last_report_indicator,
    safe_cast(transmittal_number as string) transmittal_number,
    safe_cast(fiscal_intermediary_number as string) fiscal_intermediary_number,
    safe_cast(adr_vendor_code as string) adr_vendor_code,
    safe_cast(fiscal_intermediary_create_date as date) fiscal_intermediary_create_date,
    safe_cast(utilization_code as string) utilization_code,
    safe_cast(
        notice_of_program_reimbursement_date as date
    ) notice_of_program_reimbursement_date,
    safe_cast(special_indicator as string) special_indicator,
    safe_cast(fiscal_intermediary_receipt_date as date) fiscal_intermediary_receipt_date
from {{ set_datalake_project("us_cms_hcris_staging.report") }} as t
