{{
    config(
        alias="microdados_recurso_publico_mutuario",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}


select
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(id_dap as string) id_dap,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 11 then tipo_cpf_cnpj else null end as string
    ) as cpf,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 14 then tipo_cpf_cnpj else null end as string
    ) as cnpj,
    safe_cast(tipo_beneficiario as string) tipo_beneficiario,
    safe_cast(primeiro_mutuario as string) primeiro_mutuario,
    safe_cast(sexo as string) sexo
from
    {{
        set_datalake_project(
            "br_bcb_sicor_staging.microdados_recurso_publico_mutuario"
        )
    }} as t
