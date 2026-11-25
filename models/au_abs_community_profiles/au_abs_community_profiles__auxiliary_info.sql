{{
    config(
        alias="auxiliary_info",
        schema="au_abs_community_profiles",
        materialized="table",
    )
}}
select
    safe_cast(profile as string) profile,
    safe_cast(census_year as int64) census_year,
    safe_cast(table_code as string) table_code,
    safe_cast(table_name as string) table_name,
    safe_cast(table_population as string) table_population,
    safe_cast(cell_code as string) cell_code,
    safe_cast(long_description as string) long_description,
    safe_cast(heading as string) heading,
    safe_cast(datapack_part as string) datapack_part,
    safe_cast(statistic_type as string) statistic_type,
    safe_cast(measurement_unit as string) measurement_unit
from {{ set_datalake_project("au_abs_community_profiles_staging.auxiliary_info") }} as t
