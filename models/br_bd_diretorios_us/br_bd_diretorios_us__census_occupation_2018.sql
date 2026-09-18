{{
    config(
        alias="census_occupation_2018",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_census_occupation as string) id_census_occupation,
    safe_cast(name as string) name,
    safe_cast(id_soc as string) id_soc
from
    {{ set_datalake_project("br_bd_diretorios_us_staging.census_occupation_2018") }}
    as t
