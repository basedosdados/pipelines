{{
    config(
        alias="recurso_publico_cooperado",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}

select
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(tipo_cpf_cnpj as string) tipo_cpf_cnpj,
    safe_cast(tipo_pessoa as string) tipo_pessoa,
    safe_cast(valor_parcela as float64) valor_parcela
from {{ set_datalake_project("br_bcb_sicor_staging.recurso_publico_cooperado") }} as t
