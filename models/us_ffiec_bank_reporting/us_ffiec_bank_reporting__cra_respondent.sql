{{
    config(
        schema="us_ffiec_bank_reporting",
        alias="cra_respondent",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1996, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(respondent_id as string) respondent_id,
    safe_cast(agency_id as string) agency_id,
    safe_cast(rssd_id as string) rssd_id,
    safe_cast(name as string) name,
    safe_cast(address as string) address,
    safe_cast(city as string) city,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(zip_code as string) zip_code,
    safe_cast(tax_id as string) tax_id,
    safe_cast(total_assets as float64) total_assets
from
    {{ set_datalake_project("us_ffiec_bank_reporting_staging.cra_respondent") }}
    as t
