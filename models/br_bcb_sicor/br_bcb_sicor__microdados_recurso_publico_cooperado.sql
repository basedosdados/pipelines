{{
    config(
        alias="microdados_recurso_publico_cooperado",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}

select
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    safe_cast(t.id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(t.numero_ordem as string) numero_ordem,
    safe_cast(id_programa as string) id_programa,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 11 then tipo_cpf_cnpj else null end as string
    ) as cpf,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 8 then tipo_cpf_cnpj else null end as string
    ) as cnpj,
    safe_cast(
        case
            when tipo_pessoa = 'F'
            then "Física"
            when tipo_pessoa = 'J'
            then 'Jurídica'
            else null
        end as string
    ) tipo_pessoa,

    safe_cast(valor_parcela as float64) valor_parcela
from
    {{
        set_datalake_project(
            "br_bcb_sicor_staging.microdados_recurso_publico_cooperado"
        )
    }} as t {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
