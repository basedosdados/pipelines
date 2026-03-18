{{
    config(
        alias="recurso_publico_propriedade",
        schema="br_bcb_sicor",
        materialized="table",
        partition_by={
            "field": "ano_emissao",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2026, "interval": 1},
        },
    )
}}

select
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    safe_cast(t.id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(t.numero_ordem as string) numero_ordem,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 11 then tipo_cpf_cnpj else null end as string
    ) as cpf,
    safe_cast(
        case when length(tipo_cpf_cnpj) = 8 then tipo_cpf_cnpj else null end as string
    ) as cnpj_basico,
    -- valores estranhos que vem por padrão da fonte original
    safe_cast(nullif(id_sncr, '-1') as string) id_sncr,
    safe_cast(nullif(id_nirf, '-1') as string) id_nirf,
    safe_cast(nullif(id_car, '-1') as string) id_car
from
    {{ set_datalake_project("br_bcb_sicor_staging.recurso_publico_propriedade") }}
    as t {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
