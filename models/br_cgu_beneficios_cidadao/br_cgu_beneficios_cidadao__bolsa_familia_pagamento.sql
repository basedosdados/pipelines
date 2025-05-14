{{
    config(
        alias="bolsa_familia_pagamento",
        schema="br_cgu_beneficios_cidadao",
        materialized="table",
        partition_by={
            "field": "ano_competencia",
            "data_type": "int64",
            "range": {
                "start": 2020,
                "end": 2023,
                "interval": 1,
            },
        },
        cluster_by=["sigla_uf", "ano_referencia", "mes_referencia", "id_municipio"],
    )
}}
select distinct
    safe_cast(split(mes_ref, '-')[offset(0)] as int64) as ano_competencia,
    safe_cast(split(mes_ref, '-')[offset(1)] as int64) as mes_competencia,
    safe_cast(left(mes, 4) as int64) ano_referencia,
    safe_cast(right(mes, 2) as int64) mes_referencia,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    case when cpf = '' then null else cpf end as cpf_favorecido,
    safe_cast(nis as string) nis_favorecido,
    safe_cast(nome as string) nome_favorecido,
    safe_cast(valor_beneficio as float64) valor_parcela,
from
    {{
        set_datalake_project(
            "br_cgu_beneficios_cidadao_staging.bolsa_familia_pagamento"
        )
    }} as t
