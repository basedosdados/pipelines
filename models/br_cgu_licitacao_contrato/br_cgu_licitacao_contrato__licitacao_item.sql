{{
    config(
        alias="licitacao_item",
        schema="br_cgu_licitacao_contrato",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(numero_licitacao as string) id_licitacao,
    safe_cast(codigo_ug as string) id_unidade_gestora,
    safe_cast(nome_ug as string) nome_unidade_gestora,
    safe_cast(codigo_modalidade_compra as string) id_modalidade,
    safe_cast(modalidade_compra as string) modalidade,
    safe_cast(numero_processo as string) numero_processo,
    safe_cast(codigo_orgao as string) id_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(codigo_vencedor as string) cpf_cnpj_vencedor,
    safe_cast(nome_vencedor as string) nome_vencedor,
    safe_cast(codigo_item_compra as string) id_item,
    safe_cast(descricao as string) descricao_item,
    safe_cast(quantidade_item as int64) quantidade_item,
    safe_cast(replace(valor_item, ",", ".") as float64) valor_item,
from {{ set_datalake_project("br_cgu_licitacao_contrato_staging.licitacao_item") }} as t
