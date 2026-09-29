{{
    config(
        alias="complexity_country",
        schema="world_cepii_baci",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1962, "end": 2030, "interval": 1},
        },
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(classification as string) classification,
    safe_cast(id_country as string) id_country,
    safe_cast(eci as float64) eci,
    safe_cast(coi as float64) coi,
    safe_cast(diversity as int64) diversity,
    safe_cast(growth_projection as float64) growth_projection,
    safe_cast(export_value as float64) export_value,
    safe_cast(import_value as float64) import_value
from {{ set_datalake_project("world_cepii_baci_staging.complexity_country") }} as t
