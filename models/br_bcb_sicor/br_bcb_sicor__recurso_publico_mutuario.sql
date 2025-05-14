{{
    config(
        alias="recurso_publico_mutuario",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}


select
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(indicador_sexo as int64) indicador_sexo,
    safe_cast(tipo_cpf_cnpj as string) tipo_cpf_cnpj,
    safe_cast(tipo_beneficiario as string) tipo_beneficiario,
    safe_cast(id_dap as string) id_dap
from {{ set_datalake_project("br_bcb_sicor_staging.recurso_publico_mutuario") }} as t
