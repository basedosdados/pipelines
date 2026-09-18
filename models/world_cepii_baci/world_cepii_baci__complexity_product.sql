{{
    config(
        alias="complexity_product",
        schema="world_cepii_baci",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1962, "end": 2030, "interval": 1},
        },
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(classification as string) classification,
    safe_cast(product_code as string) product_code,
    safe_cast(pci as float64) pci,
    safe_cast(export_value as float64) export_value,
    safe_cast(import_value as float64) import_value
from {{ set_datalake_project("world_cepii_baci_staging.complexity_product") }} as t
