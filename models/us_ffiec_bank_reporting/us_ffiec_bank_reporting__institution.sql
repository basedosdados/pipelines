{{
    config(
        schema="us_ffiec_bank_reporting",
        alias="institution",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(report_date as date) report_date,
    safe_cast(rssd_id as string) rssd_id,
    safe_cast(fdic_cert_id as string) fdic_cert_id,
    safe_cast(occ_charter_id as string) occ_charter_id,
    safe_cast(ots_docket_id as string) ots_docket_id,
    safe_cast(aba_routing_id as string) aba_routing_id,
    safe_cast(name as string) name,
    safe_cast(address as string) address,
    safe_cast(city as string) city,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(zip_code as string) zip_code,
    safe_cast(call_report_form_id as string) call_report_form_id,
    safe_cast(last_submission_updated_at as datetime) last_submission_updated_at
from {{ set_datalake_project("us_ffiec_bank_reporting_staging.institution") }} as t
