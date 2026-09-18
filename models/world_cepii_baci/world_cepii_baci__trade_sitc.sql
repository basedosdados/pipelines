{{
    config(
        alias="trade_sitc",
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
    safe_cast(id_country_exporter as string) id_country_exporter,
    safe_cast(id_country_importer as string) id_country_importer,
    safe_cast(product_code as string) product_code,
    safe_cast(export_value as float64) export_value,
    safe_cast(import_value as float64) import_value
from {{ set_datalake_project("world_cepii_baci_staging.trade_sitc") }} as t
