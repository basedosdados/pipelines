-- Built by models/br_bd_diretorios_us/code/clean.py from the Census CBP
-- naics-descriptions reference file for the 2012 vintage.
{{
    config(
        alias="naics_2012",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_naics as string) id_naics,
    safe_cast(name as string) name,
    safe_cast(level as int64) level,
    safe_cast(id_sector as string) id_sector,
    safe_cast(name_sector as string) name_sector,
    safe_cast(id_subsector as string) id_subsector,
    safe_cast(name_subsector as string) name_subsector,
    safe_cast(id_industry_group as string) id_industry_group,
    safe_cast(name_industry_group as string) name_industry_group,
    safe_cast(id_naics_industry as string) id_naics_industry,
    safe_cast(name_naics_industry as string) name_naics_industry
from {{ set_datalake_project("br_bd_diretorios_us_staging.naics_2012") }} as t
