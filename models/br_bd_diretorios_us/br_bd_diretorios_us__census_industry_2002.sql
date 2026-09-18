{{
    config(
        alias="census_industry_2002",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_census_industry as string) id_census_industry,
    safe_cast(name as string) name,
    safe_cast(id_naics as string) id_naics
from {{ set_datalake_project("br_bd_diretorios_us_staging.census_industry_2002") }} as t
