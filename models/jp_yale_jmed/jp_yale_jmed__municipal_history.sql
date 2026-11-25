{{
    config(
        alias="municipal_history",
        schema="jp_yale_jmed",
        materialized="table",
    )
}}
select
    safe_cast(predecessor_id as string) predecessor_id,
    safe_cast(predecessor_code as string) predecessor_code,
    safe_cast(successor_id as string) successor_id,
    safe_cast(successor_code as string) successor_code,
    safe_cast(predecessor_name as string) predecessor_name,
    safe_cast(successor_name as string) successor_name,
    safe_cast(change_date as date) change_date,
    safe_cast(change_type as string) change_type
from {{ set_datalake_project("jp_yale_jmed_staging.municipal_history") }} as t
