{{
    config(
        alias="disaster_declaration",
        schema="us_fema_openfema",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1953, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_id as string) state_id,
    safe_cast(county_id as string) county_id,
    safe_cast(disaster_number as string) disaster_number,
    safe_cast(place_code as string) place_code,
    safe_cast(fema_declaration_string as string) fema_declaration_string,
    safe_cast(incident_id as string) incident_id,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(declaration_type as string) declaration_type,
    safe_cast(declaration_date as date) declaration_date,
    safe_cast(fy_declared as int64) fy_declared,
    safe_cast(incident_type as string) incident_type,
    safe_cast(declaration_title as string) declaration_title,
    safe_cast(ih_program_declared as boolean) ih_program_declared,
    safe_cast(ia_program_declared as boolean) ia_program_declared,
    safe_cast(pa_program_declared as boolean) pa_program_declared,
    safe_cast(hm_program_declared as boolean) hm_program_declared,
    safe_cast(incident_begin_date as date) incident_begin_date,
    safe_cast(incident_end_date as date) incident_end_date,
    safe_cast(disaster_closeout_date as date) disaster_closeout_date,
    safe_cast(tribal_request as boolean) tribal_request,
    safe_cast(fips_county_code as string) fips_county_code,
    safe_cast(designated_area as string) designated_area,
    safe_cast(declaration_request_number as string) declaration_request_number,
    safe_cast(declaration_request_date as date) declaration_request_date,
    safe_cast(last_ia_filing_date as date) last_ia_filing_date,
    safe_cast(region as int64) region,
    safe_cast(designated_incident_types as string) designated_incident_types,
    safe_cast(record_id as string) record_id
from
    {{ set_datalake_project("us_fema_openfema_staging.disaster_declaration") }}
    as t
