{{
    config(
        alias="contrato_item",
        schema="br_cgu_licitacao_contrato",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(numero_contrato as string) id_contrato,
    safe_cast(codigo_orgao as string) id_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(codigo_ug as string) id_ug,
    safe_cast(nome_ug as string) nome_ug,
    safe_cast(codigo_item_compra as string) id_item,
    safe_cast(descricao_item_compra as string) descricao_item,
    safe_cast(descricao_complementar_item_compra as string) descricao_complementar_item,
    safe_cast(quantidade_item as int64) quantidade_item,
    safe_cast(replace(valor_item, ",", ".") as float64) valor_item,
from {{ set_datalake_project("br_cgu_licitacao_contrato_staging.contrato_item") }} as t
