{{
    config(
        alias="auxilio_emergencial",
        schema="br_cgu_beneficios_cidadao",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {
                "start": 2020,
                "end": 2021,
                "interval": 1,
            },
        },
        cluster_by=["sigla_uf", "id_municipio"],
    )
}}

select
    safe_cast(split(mes, '-')[offset(0)] as int64) as ano,
    safe_cast(split(mes, '-')[offset(1)] as int64) as mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(nis_beneficiario as string) nis_beneficiario,
    safe_cast(cpf_beneficiario as string) cpf_beneficiario,
    safe_cast(nome_beneficiario as string) nome_beneficiario,
    safe_cast(nis_responsavel as string) nis_responsavel,
    safe_cast(cpf_responsavel as string) cpf_responsavel,
    safe_cast(nome_responsavel as string) nome_responsavel,
    safe_cast(enquadramento as string) enquadramento,
    safe_cast(parcela as string) parcela,
    safe_cast(observacao as string) observacao,
    safe_cast(valor_beneficio as float64) valor_beneficio,
from
    {{ set_datalake_project("br_cgu_beneficios_cidadao_staging.auxilio_emergencial") }}
    as t
