{{
    config(
        schema="br_bd_diretorios_de",
        alias="party",
        materialized="table",
    )
}}

select
    safe_cast(id_party as string) id_party,
    safe_cast(name as string) name,
    safe_cast(name_short as string) name_short,
    safe_cast(family as string) family,
    safe_cast(left_right as float64) left_right,
    safe_cast(parlgov_party_id as string) parlgov_party_id,
    safe_cast(is_far_right as string) is_far_right,
    safe_cast(is_far_left as string) is_far_left,
    safe_cast(is_cdu_csu as string) is_cdu_csu,
    safe_cast(category as string) category
from {{ set_datalake_project("br_bd_diretorios_de_staging.party") }} as t
