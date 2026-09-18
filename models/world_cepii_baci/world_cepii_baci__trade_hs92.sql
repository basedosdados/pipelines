{{
    config(
        alias="trade_hs92",
        schema="world_cepii_baci",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1995, "end": 2030, "interval": 1},
        },
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(id_country_exporter as string) id_country_exporter,
    safe_cast(id_country_importer as string) id_country_importer,
    safe_cast(product_code as string) product_code,
    safe_cast(value as float64) value,
    safe_cast(quantity as float64) quantity
from {{ set_datalake_project("world_cepii_baci_staging.trade_hs92") }} as t
