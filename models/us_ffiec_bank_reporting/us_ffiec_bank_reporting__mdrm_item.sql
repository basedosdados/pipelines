{{
    config(
        schema="us_ffiec_bank_reporting",
        alias="mdrm_item",
        materialized="table",
    )
}}


select
    safe_cast(item_code as string) item_code,
    safe_cast(mnemonic as string) mnemonic,
    safe_cast(item_number as string) item_number,
    safe_cast(name as string) name,
    safe_cast(description as string) description,
    safe_cast(item_type as string) item_type,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(is_flag as string) is_flag,
    safe_cast(is_confidential as string) is_confidential,
    safe_cast(reporting_form as string) reporting_form,
    safe_cast(start_date as date) start_date,
    safe_cast(end_date as date) end_date,
    safe_cast(series_glossary as string) series_glossary
from {{ set_datalake_project("us_ffiec_bank_reporting_staging.mdrm_item") }} as t
