{{
    config(
        schema="br_rf_arrecadacao",
        alias="itr",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2017, "end": 2024, "interval": 1},
        },
        cluster_by=["mes"],
    )
}}
select
    safe_cast(itr.ano as int64) ano,
    safe_cast(itr.mes as int64) mes,
    safe_cast(itr.sigla_uf as string) sigla_uf,
    safe_cast(m.id_municipio as string) id_municipio,
    safe_cast(itr.valor_arrecadado as float64) valor_arrecadado,
from {{ set_datalake_project("br_rf_arrecadacao_staging.itr") }} itr
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` m
    on itr.cidade = m.nome
    and itr.sigla_uf = m.sigla_uf
