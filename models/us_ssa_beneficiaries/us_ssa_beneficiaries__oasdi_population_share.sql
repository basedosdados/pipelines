{{
    config(
        alias="oasdi_population_share",
        schema="us_ssa_beneficiaries",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1998, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_id as string) state_id,
    safe_cast(state_or_area as string) state_or_area,
    safe_cast(population_group as string) population_group,
    safe_cast(population as int64) population,
    safe_cast(population_note as string) population_note,
    safe_cast(percentage_receiving_oasdi as float64) percentage_receiving_oasdi,
    safe_cast(percentage_receiving_oasdi_note as string) percentage_receiving_oasdi_note
from
    {{ set_datalake_project("us_ssa_beneficiaries_staging.oasdi_population_share") }}
    as t
