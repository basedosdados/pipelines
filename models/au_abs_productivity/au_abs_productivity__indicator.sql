{{
    config(
        schema="au_abs_productivity",
        alias="indicator",
        materialized="table",
    )
}}


select
    safe_cast(indicator_id as string) indicator_id,
    safe_cast(table_no as string) table_no,
    safe_cast(industry_code as string) industry_code,
    safe_cast(state_id as string) state_id,
    safe_cast(table_name as string) table_name,
    safe_cast(table_group as string) table_group,
    safe_cast(section as string) section,
    safe_cast(item as string) item,
    safe_cast(item_path as string) item_path,
    safe_cast(industry_name as string) industry_name,
    safe_cast(state_name as string) state_name,
    safe_cast(basis as string) basis,
    safe_cast(unit as string) unit,
    safe_cast(first_financial_year as string) first_financial_year,
    safe_cast(last_financial_year as string) last_financial_year
from {{ set_datalake_project("au_abs_productivity_staging.indicator") }} as t
