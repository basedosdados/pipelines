{{
    config(
        schema="br_bcb_taxa_selic", materialized="table", labels={"tema": "economia"}
    )
}}

select safe_cast(data as date) data, safe_cast(valor as float64) valor,
from {{ set_datalake_project("br_bcb_taxa_selic_staging.taxa_selic") }} as t
