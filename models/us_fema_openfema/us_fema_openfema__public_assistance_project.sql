{{
    config(
        alias="public_assistance_project",
        schema="us_fema_openfema",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1998, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_id as string) state_id,
    safe_cast(county_id as string) county_id,
    safe_cast(gm_project_id as string) gm_project_id,
    safe_cast(disaster_number as string) disaster_number,
    safe_cast(pw_number as int64) pw_number,
    safe_cast(applicant_id as string) applicant_id,
    safe_cast(gm_applicant_id as string) gm_applicant_id,
    safe_cast(declaration_date as date) declaration_date,
    safe_cast(incident_type as string) incident_type,
    safe_cast(application_title as string) application_title,
    safe_cast(damage_category_code as string) damage_category_code,
    safe_cast(damage_category_descrip as string) damage_category_descrip,
    safe_cast(project_status as string) project_status,
    safe_cast(project_process_step as string) project_process_step,
    safe_cast(project_size as string) project_size,
    safe_cast(county as string) county,
    safe_cast(county_code as string) county_code,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(project_amount as float64) project_amount,
    safe_cast(federal_share_obligated as float64) federal_share_obligated,
    safe_cast(total_obligated as float64) total_obligated,
    safe_cast(last_obligation_date as datetime) last_obligation_date,
    safe_cast(first_obligation_date as datetime) first_obligation_date,
    safe_cast(mitigation_amount as float64) mitigation_amount
from
    {{ set_datalake_project("us_fema_openfema_staging.public_assistance_project") }}
    as t
