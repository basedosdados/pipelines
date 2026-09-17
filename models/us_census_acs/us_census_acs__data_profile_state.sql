{{
    config(
        schema="us_census_acs",
        alias="data_profile_state",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2030, "interval": 1},
        },
        cluster_by=["id_state", "variable_code"],
    )
}}

select
    safe_cast(ano as int64) year,
    safe_cast(period as string) period,
    safe_cast(id_state as string) id_state,
    safe_cast(variable_code as string) variable_code,
    safe_cast(estimate as float64) estimate,
    safe_cast(margin_of_error as float64) margin_of_error
from {{ set_datalake_project("us_census_acs_staging.data_profile_state") }} as t
