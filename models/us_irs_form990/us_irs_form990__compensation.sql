{{
    config(
        schema="us_irs_form990",
        alias="compensation",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2040, "interval": 1},
        },
    )
}}

-- Atualizado em 2026-09-03
-- Restricted to the filings kept in return_financial (one per ein,
-- year and form_type), so amended or re-released returns do not
-- list their officers twice.
select
    safe_cast(year as int64) year,
    safe_cast(ein as string) ein,
    safe_cast(form_type as string) form_type,
    safe_cast(object_id as string) object_id,
    safe_cast(line_number as string) line_number,
    safe_cast(person_name as string) person_name,
    safe_cast(business_name as string) business_name,
    safe_cast(title as string) title,
    safe_cast(average_hours_per_week as float64) average_hours_per_week,
    safe_cast(average_hours_per_week_related as float64) average_hours_per_week_related,
    safe_cast(
        is_individual_trustee_or_director as boolean
    ) is_individual_trustee_or_director,
    safe_cast(is_institutional_trustee as boolean) is_institutional_trustee,
    safe_cast(is_officer as boolean) is_officer,
    safe_cast(is_key_employee as boolean) is_key_employee,
    safe_cast(
        is_highest_compensated_employee as boolean
    ) is_highest_compensated_employee,
    safe_cast(is_former as boolean) is_former,
    safe_cast(
        reportable_compensation_from_organization as float64
    ) reportable_compensation_from_organization,
    safe_cast(
        reportable_compensation_from_related as float64
    ) reportable_compensation_from_related,
    safe_cast(other_compensation as float64) other_compensation,
    safe_cast(employee_benefit_contributions as float64) employee_benefit_contributions
from {{ set_datalake_project("us_irs_form990_staging.compensation") }} as t
where
    safe_cast(object_id as string)
    in (select object_id from {{ ref("us_irs_form990__return_financial") }})
