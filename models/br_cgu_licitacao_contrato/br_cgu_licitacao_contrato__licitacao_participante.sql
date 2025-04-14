{{
    config(
        alias="licitacao_participante",
        schema="br_cgu_licitacao_contrato",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    safe_cast(numero_licitacao as string) as id_licitacao,
    safe_cast(codigo_ug as string) as id_unidade_gestora,
    safe_cast(nome_ug as string) as nome_unidade_gestora,
    safe_cast(codigo_modalidade_compra as string) as id_modalidade_compra,
    safe_cast(modalidade_compra as string) as modalidade_compra,
    safe_cast(numero_processo as string) as numero_processo,
    safe_cast(codigo_orgao as string) as id_orgao,
    safe_cast(nome_orgao as string) as nome_orgao,
    safe_cast(codigo_item_compra as string) as id_item_compra,
    safe_cast(descricao_item_compra as string) as descricao_item_compra,
    safe_cast(codigo_participante as string) as cpf_cnpj_participante,
    safe_cast(nome_participante as string) as nome_participante,
    safe_cast(
        case
            when flag_vencedor = 'SIM' then true when flag_vencedor = 'NÃO' then false
        end as boolean
    ) as vencedor
from
    {{
        set_datalake_project(
            "br_cgu_licitacao_contrato_staging.licitacao_participante"
        )
    }}
