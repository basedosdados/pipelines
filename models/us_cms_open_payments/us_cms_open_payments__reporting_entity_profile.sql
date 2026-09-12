{{
    config(
        schema="us_cms_open_payments",
        alias="reporting_entity_profile",
        materialized="table",
    )
}}


select
    safe_cast(reporting_entity_id as string) reporting_entity_id,
    safe_cast(name as string) name,
    safe_cast(state as string) state,
    safe_cast(country as string) country,
    safe_cast(alternate_name_1 as string) alternate_name_1,
    safe_cast(alternate_name_2 as string) alternate_name_2,
    safe_cast(alternate_name_3 as string) alternate_name_3,
    safe_cast(alternate_name_4 as string) alternate_name_4,
    safe_cast(alternate_name_5 as string) alternate_name_5
from
    {{ set_datalake_project("us_cms_open_payments_staging.reporting_entity_profile") }}
    as t
