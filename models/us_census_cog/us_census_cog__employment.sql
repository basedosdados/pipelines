{{
    config(
        schema="us_census_cog",
        alias="employment",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1992, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(government_id as string) government_id,
    safe_cast(government_id_govs as string) government_id_govs,
    safe_cast(state_id as string) state_id,
    safe_cast(government_type as string) government_type,
    safe_cast(function_code as string) function_code,
    safe_cast(full_time_employees as int64) full_time_employees,
    safe_cast(full_time_employees_flag as string) full_time_employees_flag,
    safe_cast(full_time_payroll as int64) full_time_payroll,
    safe_cast(full_time_payroll_flag as string) full_time_payroll_flag,
    safe_cast(part_time_employees as int64) part_time_employees,
    safe_cast(part_time_employees_flag as string) part_time_employees_flag,
    safe_cast(part_time_payroll as int64) part_time_payroll,
    safe_cast(part_time_payroll_flag as string) part_time_payroll_flag,
    safe_cast(part_time_hours as int64) part_time_hours,
    safe_cast(full_time_equivalent_employees as int64) full_time_equivalent_employees
from {{ set_datalake_project("us_census_cog_staging.employment") }} as t
