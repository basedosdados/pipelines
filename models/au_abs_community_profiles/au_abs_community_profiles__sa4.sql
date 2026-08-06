{{
    config(
        alias="sa4",
        schema="au_abs_community_profiles",
        materialized="table",
        partition_by={
            "field": "census_year",
            "data_type": "int64",
            "range": {"start": 2011, "end": 2027, "interval": 1},
        },
    )
}}
select
    safe_cast(census_year as int64) census_year,
    safe_cast(id_sa4 as string) id_sa4,
    safe_cast(profile as string) profile,
    safe_cast(table_code as string) table_code,
    safe_cast(cell_code as string) cell_code,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_abs_community_profiles_staging.sa4") }} as t
