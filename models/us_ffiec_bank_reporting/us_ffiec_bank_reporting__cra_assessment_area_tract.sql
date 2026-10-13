{{
    config(
        schema="us_ffiec_bank_reporting",
        alias="cra_assessment_area_tract",
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
    safe_cast(state_id as string) state_id,
    safe_cast(county_id as string) county_id,
    safe_cast(msa_md_id as string) msa_md_id,
    safe_cast(census_tract_id as string) census_tract_id,
    safe_cast(assessment_area_id as string) assessment_area_id,
    safe_cast(is_partial_county as string) is_partial_county,
    safe_cast(is_split_county as string) is_split_county,
    safe_cast(population_classification as string) population_classification,
    safe_cast(tract_income_group as string) tract_income_group
from
    {{
        set_datalake_project(
            "us_ffiec_bank_reporting_staging.cra_assessment_area_tract"
        )
    }} as t
