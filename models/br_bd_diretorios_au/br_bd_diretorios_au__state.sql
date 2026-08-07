{{
    config(
        alias="state",
        schema="br_bd_diretorios_au",
        materialized="table",
    )
}}
select
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation as string) abbreviation,
    safe_cast(name as string) name,
    safe_cast(area_albers_sqkm as float64) area_albers_sqkm
from {{ set_datalake_project("br_bd_diretorios_au_staging.state") }} as t
