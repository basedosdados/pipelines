{{
    config(
        schema="us_ffiec_bank_reporting",
        alias="holding_company",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1986, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(report_date as date) report_date,
    safe_cast(rssd_id as string) rssd_id,
    safe_cast(name as string) name,
    safe_cast(short_name as string) short_name,
    safe_cast(address as string) address,
    safe_cast(city as string) city,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(zip_code as string) zip_code,
    safe_cast(county_id as string) county_id,
    safe_cast(tax_id as string) tax_id,
    safe_cast(charter_type_id as string) charter_type_id,
    safe_cast(organization_type_id as string) organization_type_id,
    safe_cast(primary_activity_id as string) primary_activity_id,
    safe_cast(federal_reserve_district_id as string) federal_reserve_district_id,
    safe_cast(bank_count as int64) bank_count,
    safe_cast(is_financial_holding_company as string) is_financial_holding_company,
    safe_cast(is_savings_loan_holding_company as string) is_savings_loan_holding_company
from {{ set_datalake_project("us_ffiec_bank_reporting_staging.holding_company") }} as t
