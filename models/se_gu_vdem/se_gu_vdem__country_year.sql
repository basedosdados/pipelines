{{
    config(
        alias="country_year",
        schema="se_gu_vdem",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1789, "end": 2031, "interval": 1},
        },
        cluster_by=["variable_id", "country_id"],
    )
}}
select
    safe_cast(year as int64) year,
    safe_cast(country_id as string) country_id,
    safe_cast(variable_id as string) variable_id,
    safe_cast(value as float64) value,
    safe_cast(value_text as string) value_text,
    safe_cast(code_low as float64) code_low,
    safe_cast(code_high as float64) code_high,
    safe_cast(sd as float64) sd,
    safe_cast(osp as float64) osp,
    safe_cast(osp_code_low as float64) osp_code_low,
    safe_cast(osp_code_high as float64) osp_code_high,
    safe_cast(osp_sd as float64) osp_sd,
    safe_cast(ord as float64) ord,
    safe_cast(ord_code_low as float64) ord_code_low,
    safe_cast(ord_code_high as float64) ord_code_high,
    safe_cast(mean as float64) mean,
    safe_cast(coder_count as int64) coder_count
from {{ set_datalake_project("se_gu_vdem_staging.country_year") }} as t
