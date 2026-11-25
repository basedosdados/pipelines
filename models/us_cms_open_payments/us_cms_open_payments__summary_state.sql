{{
    config(
        schema="us_cms_open_payments",
        alias="summary_state",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2019, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_code as string) country_code,
    safe_cast(country_name as string) country_name,
    safe_cast(state_code as string) state_code,
    safe_cast(state_name as string) state_name,
    safe_cast(payment_type as string) payment_type,
    safe_cast(recipient_type as string) recipient_type,
    safe_cast(physician_count as int64) physician_count,
    safe_cast(
        non_physician_practitioner_count as int64
    ) non_physician_practitioner_count,
    safe_cast(teaching_hospital_count as int64) teaching_hospital_count,
    safe_cast(payment_amount_total_physician as float64) payment_amount_total_physician,
    safe_cast(
        payment_amount_total_non_physician_practitioner as float64
    ) payment_amount_total_non_physician_practitioner,
    safe_cast(
        payment_amount_total_teaching_hospital as float64
    ) payment_amount_total_teaching_hospital,
    safe_cast(payment_amount_mean_physician as float64) payment_amount_mean_physician,
    safe_cast(
        payment_amount_mean_non_physician_practitioner as float64
    ) payment_amount_mean_non_physician_practitioner,
    safe_cast(
        payment_amount_mean_teaching_hospital as float64
    ) payment_amount_mean_teaching_hospital,
    safe_cast(
        payment_amount_median_physician as float64
    ) payment_amount_median_physician,
    safe_cast(
        payment_amount_median_non_physician_practitioner as float64
    ) payment_amount_median_non_physician_practitioner,
    safe_cast(
        payment_amount_median_teaching_hospital as float64
    ) payment_amount_median_teaching_hospital,
    safe_cast(payment_count_total_physician as int64) payment_count_total_physician,
    safe_cast(
        payment_count_total_non_physician_practitioner as int64
    ) payment_count_total_non_physician_practitioner,
    safe_cast(
        payment_count_total_teaching_hospital as int64
    ) payment_count_total_teaching_hospital,
    safe_cast(payment_count_mean_physician as float64) payment_count_mean_physician,
    safe_cast(
        payment_count_mean_non_physician_practitioner as float64
    ) payment_count_mean_non_physician_practitioner,
    safe_cast(
        payment_count_mean_teaching_hospital as float64
    ) payment_count_mean_teaching_hospital,
    safe_cast(payment_count_median_physician as float64) payment_count_median_physician,
    safe_cast(
        payment_count_median_non_physician_practitioner as float64
    ) payment_count_median_non_physician_practitioner,
    safe_cast(
        payment_count_median_teaching_hospital as float64
    ) payment_count_median_teaching_hospital
from {{ set_datalake_project("us_cms_open_payments_staging.summary_state") }} as t
