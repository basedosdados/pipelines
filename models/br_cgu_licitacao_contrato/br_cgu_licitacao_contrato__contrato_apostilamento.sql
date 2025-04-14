{{ config(alias="contrato_apostilamento", schema="br_cgu_licitacao_contrato") }}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(numero_contrato as string) id_contrato,
    safe_cast(codigo_orgao_superior as string) id_orgao_superior,
    safe_cast(nome_orgao_superior as string) nome_orgao_superior,
    safe_cast(codigo_orgao as string) id_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(codigo_ug as string) id_unidade_gestora,
    safe_cast(nome_ug as string) nome_unidade_gestora,
    safe_cast(numero_apostilamento as string) id_apostilamento,
    safe_cast(descricao_apostilamento as string) descricao_apostilamento,
    safe_cast(situacao_apostilamento as string) situacao_apostilamento,
    safe_cast(parse_date('%d/%m/%Y', data_de_inclusao) as date) data_apostilamento,
    safe_cast(replace(valor_apostilamento, ",", ".") as float64) valor_apostilamento,
from
    {{
        set_datalake_project(
            "br_cgu_licitacao_contrato_staging.contrato_apostilamento"
        )
    }} as t
