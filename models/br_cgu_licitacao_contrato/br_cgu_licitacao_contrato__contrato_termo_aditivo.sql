{{
    config(
        alias="contrato_termo_aditivo",
        schema="br_cgu_licitacao_contrato",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(numero_contrato as string) id_contrato,
    safe_cast(codigo_orgao_superior as string) id_orgao_superior,
    safe_cast(nome_orgao_superior as string) nome_orgao_superior,
    safe_cast(codigo_orgao as string) id_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(codigo_ug as string) id_ug,
    safe_cast(nome_ug as string) nome_ug,
    safe_cast(numero_termo_aditivo as string) id_termo_aditivo,
    safe_cast(parse_date('%d/%m/%Y', data_publicacao) as date) data_publicacao_dou,
    safe_cast(objeto as string) objeto,
from
    {{
        set_datalake_project(
            "br_cgu_licitacao_contrato_staging.contrato_termo_aditivo"
        )
    }} as t
