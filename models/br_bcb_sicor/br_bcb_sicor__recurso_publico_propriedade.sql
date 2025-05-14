{{
    config(
        alias="recurso_publico_propriedade",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}

select
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(tipo_cpf_cnpj as string) tipo_cpf_cnpj,
    safe_cast(id_sncr as string) id_sncr,
    safe_cast(id_nirf as string) id_nirf,
    safe_cast(id_car as string) id_car
from {{ set_datalake_project("br_bcb_sicor_staging.recurso_publico_propriedade") }} as t
