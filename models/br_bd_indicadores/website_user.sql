select
    safe_cast(reference_date as date) reference_date,
    safe_cast(users_1_day as int64) users_1_day,
    safe_cast(users_7_days as int64) users_7_days,
    safe_cast(users_14_days as int64) users_14_days,
    safe_cast(users_28_days as int64) users_28_days,
    safe_cast(users_30_days as int64) users_30_days,
    safe_cast(new_users as int64) new_users
from {{ set_datalake_project("br_bd_indicadores_staging.website_user") }} as t
