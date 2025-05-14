{{
    config(
        alias="licitacao_empenho",
        schema="br_cgu_licitacao_contrato",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(numero_processo as string) id_processo,
    safe_cast(numero_licitacao as string) id_licitacao,
    safe_cast(codigo_ug as string) id_unidade_gestora,
    safe_cast(nome_ug as string) nome_unidade_gestora,
    safe_cast(codigo_modalidade_compra as string) id_modalidade,
    safe_cast(modalidade_compra as string) modalidade,
    safe_cast(observacao_empenho as string) observacao,
    safe_cast(codigo_empenho as string) id_empenho,
    safe_cast(parse_date('%d/%m/%Y', data_emissao_empenho) as date) data_emissao,
    safe_cast(replace(valor_empenho, ",", ".") as float64) valor,
from
    {{ set_datalake_project("br_cgu_licitacao_contrato_staging.licitacao_empenho") }}
    as t
