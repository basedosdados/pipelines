{{
    config(
        schema="us_osha_enforcement",
        alias="inspection",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1970, "end": 2035, "interval": 1},
        },
        cluster_by=["site_state", "naics_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(inspection_id as string) inspection_id,
    safe_cast(reporting_office_id as string) reporting_office_id,
    safe_cast(establishment_key as string) establishment_key,
    safe_cast(establishment_name as string) establishment_name,
    safe_cast(site_address as string) site_address,
    safe_cast(site_city as string) site_city,
    safe_cast(site_state as string) site_state,
    safe_cast(site_zip_code as string) site_zip_code,
    safe_cast(mailing_address as string) mailing_address,
    safe_cast(mailing_city as string) mailing_city,
    safe_cast(mailing_state as string) mailing_state,
    safe_cast(mailing_zip_code as string) mailing_zip_code,
    safe_cast(sic_code as string) sic_code,
    safe_cast(naics_code as string) naics_code,
    safe_cast(owner_type as string) owner_type,
    safe_cast(owner_code as string) owner_code,
    safe_cast(inspection_type as string) inspection_type,
    safe_cast(inspection_scope as string) inspection_scope,
    safe_cast(no_inspection_reason as string) no_inspection_reason,
    safe_cast(safety_health as string) safety_health,
    safe_cast(advance_notice as string) advance_notice,
    safe_cast(union_status as string) union_status,
    safe_cast(safety_program_manufacturing as string) safety_program_manufacturing,
    safe_cast(safety_program_construction as string) safety_program_construction,
    safe_cast(safety_program_maritime as string) safety_program_maritime,
    safe_cast(health_program_manufacturing as string) health_program_manufacturing,
    safe_cast(health_program_construction as string) health_program_construction,
    safe_cast(health_program_maritime as string) health_program_maritime,
    safe_cast(migrant_labor as string) migrant_labor,
    safe_cast(employees_in_establishment as int64) employees_in_establishment,
    safe_cast(open_date as date) open_date,
    safe_cast(closing_conference_date as date) closing_conference_date,
    safe_cast(close_case_date as date) close_case_date,
    safe_cast(case_modified_date as date) case_modified_date
from {{ set_datalake_project("us_osha_enforcement_staging.inspection") }} as t
