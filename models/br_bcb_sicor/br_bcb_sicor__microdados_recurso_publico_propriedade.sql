{{
    config(
        alias="microdados_recurso_publico_propriedade",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}

select
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 11 then tipo_cpf_cnpj else null end as string
    ) as cpf,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 14 then tipo_cpf_cnpj else null end as string
    ) as cnpj,
    -- valores estranhos que vem por padrão da fonte original
    safe_cast(nullif(id_sncr, '-1') as string) id_sncr,
    safe_cast(nullif(id_nirf, '-1') as string) id_nirf,
    safe_cast(nullif(id_car, '-1') as string) id_car
from
    {{
        set_datalake_project(
            "br_bcb_sicor_staging.microdados_recurso_publico_propriedade"
        )
    }} as t
