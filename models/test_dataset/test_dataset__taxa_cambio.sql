{{
    config(
        schema="test_dataset",
        alias="taxa_cambio",
        materialized="table",
    )
}}

select safe_cast(data as date) as data, safe_cast(valor as float64) as valor
from {{ set_datalake_project("test_dataset_staging.taxa_cambio") }} as t
