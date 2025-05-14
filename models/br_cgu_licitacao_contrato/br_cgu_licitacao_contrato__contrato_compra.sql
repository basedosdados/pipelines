{{
    config(
        alias="contrato_compra",
        schema="br_cgu_licitacao_contrato",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(numero_do_contrato as string) id_contrato,
    safe_cast(objeto as string) objeto,
    safe_cast(fundamento_legal as string) fundamento_legal,
    safe_cast(modalidade_compra as string) modalidade,
    safe_cast(situacao_contrato as string) situacao_contrato,
    safe_cast(codigo_orgao_superior as string) id_orgao_superior,
    safe_cast(nome_orgao_superior as string) nome_orgao_superior,
    safe_cast(codigo_orgao as string) id_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(codigo_ug as string) id_unidade_gestora,
    safe_cast(nome_ug as string) nome_unidade_gestora,
    safe_cast(
        parse_date('%d/%m/%Y', data_assinatura_contrato) as date
    ) data_assinatura_contrato,
    safe_cast(parse_date('%d/%m/%Y', data_publicacao_dou) as date) data_publicacao_dou,
    safe_cast(
        parse_date('%d/%m/%Y', data_inicio_vigencia) as date
    ) data_inicio_vigencia,
    safe_cast(parse_date('%d/%m/%Y', data_fim_vigencia) as date) data_fim_vigencia,
    safe_cast(codigo_contratado as string) cpf_cnpj_contratado,
    safe_cast(nome_contratado as string) nome_contratado,
    safe_cast(numero_licitacao as string) id_licitacao,
    safe_cast(codigo_ug_licitacao as string) id_unidade_gestora_licitacao,
    safe_cast(nome_ug_licitacao as string) nome_unidade_gestora_licitacao,
    safe_cast(codigo_modalidade_compra_licitacao as string) id_modalidade_licitacao,
    safe_cast(modalidade_compra_licitacao as string) modalidade_licitacao,
    safe_cast(replace(valor_inicial_compra, ",", ".") as float64) valor_inicial_compra,
    safe_cast(replace(valor_final_compra, ",", ".") as float64) valor_final_compra,

from
    {{ set_datalake_project("br_cgu_licitacao_contrato_staging.contrato_compra") }} as t
