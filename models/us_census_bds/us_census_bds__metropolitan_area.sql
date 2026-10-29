{{
    config(
        schema="us_census_bds",
        alias="metropolitan_area",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1978, "end": 2030, "interval": 1},
        },
        cluster_by=["breakdown", "cbsa_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(breakdown as string) breakdown,
    safe_cast(cbsa_id as string) cbsa_id,
    safe_cast(naics_id as string) naics_id,
    safe_cast(firm_age as string) firm_age,
    safe_cast(firm_size as string) firm_size,
    safe_cast(initial_firm_size as string) initial_firm_size,
    safe_cast(establishment_age as string) establishment_age,
    safe_cast(firms as int64) firms,
    safe_cast(establishments as int64) establishments,
    safe_cast(employment as int64) employment,
    safe_cast(dhs_denominator as int64) dhs_denominator,
    safe_cast(establishment_entries as int64) establishment_entries,
    safe_cast(establishment_entry_rate as float64) establishment_entry_rate,
    safe_cast(establishment_exits as int64) establishment_exits,
    safe_cast(establishment_exit_rate as float64) establishment_exit_rate,
    safe_cast(job_creation as int64) job_creation,
    safe_cast(job_creation_births as int64) job_creation_births,
    safe_cast(job_creation_continuers as int64) job_creation_continuers,
    safe_cast(job_creation_rate_births as float64) job_creation_rate_births,
    safe_cast(job_creation_rate as float64) job_creation_rate,
    safe_cast(job_destruction as int64) job_destruction,
    safe_cast(job_destruction_deaths as int64) job_destruction_deaths,
    safe_cast(job_destruction_continuers as int64) job_destruction_continuers,
    safe_cast(job_destruction_rate_deaths as float64) job_destruction_rate_deaths,
    safe_cast(job_destruction_rate as float64) job_destruction_rate,
    safe_cast(net_job_creation as int64) net_job_creation,
    safe_cast(net_job_creation_rate as float64) net_job_creation_rate,
    safe_cast(reallocation_rate as float64) reallocation_rate,
    safe_cast(firm_death_firms as int64) firm_death_firms,
    safe_cast(firm_death_establishments as int64) firm_death_establishments,
    safe_cast(firm_death_employment as int64) firm_death_employment,
    safe_cast(suppression_flags as string) suppression_flags
from {{ set_datalake_project("us_census_bds_staging.metropolitan_area") }} as t
