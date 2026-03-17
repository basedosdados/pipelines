{{
    config(
        alias="recurso_publico_mutuario",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}


select
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    safe_cast(t.id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(
        case when regexp_contains(id_dap, '^0+$') then null else id_dap end as string
    ) id_dap,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 11 then tipo_cpf_cnpj else null end as string
    ) as cpf,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 14 then tipo_cpf_cnpj else null end as string
    ) as cnpj,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 8 then tipo_cpf_cnpj else null end as string
    ) as cnpj_basico,
    safe_cast(tipo_beneficiario as string) tipo_beneficiario,
    safe_cast(primeiro_mutuario as string) primeiro_mutuario,
    safe_cast(sexo as string) sexo
from
    {{ set_datalake_project("br_bcb_sicor_staging.recurso_publico_mutuario") }}
    as t {{ add_ano_mes_operacao_data(["id_referencia_bacen"]) }}
