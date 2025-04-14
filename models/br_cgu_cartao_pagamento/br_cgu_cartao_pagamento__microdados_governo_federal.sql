{{
    config(
        alias="microdados_governo_federal",
        schema="br_cgu_cartao_pagamento",
        materialized="table",
    )
}}

select
    safe_cast(ano_extrato as int64) ano_extrato,
    safe_cast(mes_extrato as int64) mes_extrato,
    safe_cast(codigo_orgao_superior as string) codigo_orgao_superior,
    initcap(nome_orgao_superior) nome_orgao_superior,
    safe_cast(codigo_orgao as string) codigo_orgao,
    initcap(nome_orgao) nome_orgao,
    safe_cast(codigo_unidade_gestora as string) codigo_unidade_gestora,
    initcap(nome_unidade_gestora) nome_unidade_gestora,
    safe_cast(cpf_portador as string) cpf_portador,
    initcap(nome_portador) nome_portador,
    case
        when cnpj_ou_cpf_favorecido = '-2'
        then 'NÃO SE APLICA'
        when cnpj_ou_cpf_favorecido = '-11'
        then 'Sigiloso'
        when cnpj_ou_cpf_favorecido = '-1'
        then 'SEM INFORMACAO'
        else cnpj_ou_cpf_favorecido
    end as cnpj_cpf_favorecido,
    initcap(nome_favorecido) nome_favorecido,
    safe_cast(transacao as string) transacao,
    parse_date("%d/%m/%Y", data_transacao) data_transacao,
    safe_cast(valor_transacao as float64) valor_transacao,
from
    {{
        set_datalake_project(
            "br_cgu_cartao_pagamento_staging.microdados_governo_federal"
        )
    }} as t
