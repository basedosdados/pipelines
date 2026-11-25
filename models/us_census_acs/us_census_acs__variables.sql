{{ config(schema="us_census_acs", alias="variables", materialized="table") }}

select
    safe_cast(variable_code as string) code,
    safe_cast(profile as string) profile,
    safe_cast(profile_name as string) profile_name,
    safe_cast(line_number as string) line_number,
    safe_cast(label as string) label,
    safe_cast(concept as string) concept,
    safe_cast(universe as string) universe,
    safe_cast(unit as string) unit,
    safe_cast(is_percent as string) is_percent
from {{ set_datalake_project("us_census_acs_staging.variables") }} as t
