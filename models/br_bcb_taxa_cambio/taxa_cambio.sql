{{
    config(
        schema="br_bcb_taxa_cambio", materialized="table", labels={"tema": "economia"}
    )
}}
select
    safe_cast(ano as string) ano,
    safe_cast(data_cotacao as date) data_cotacao,
    safe_cast(hora_cotacao as time) hora_cotacao,
    safe_cast(moeda as string) moeda,
    safe_cast(tipo_moeda as string) tipo_moeda,
    safe_cast(tipo_boletim as string) tipo_boletim,
    safe_cast(paridade_compra as float64) paridade_compra,
    safe_cast(paridade_venda as float64) paridade_venda,
    safe_cast(cotacao_compra as float64) cotacao_compra,
    safe_cast(cotacao_venda as float64) cotacao_venda
from {{ set_datalake_project("br_bcb_taxa_cambio_staging.taxa_cambio") }} as t
