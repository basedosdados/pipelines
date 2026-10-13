{{
    config(
        alias="favorecido",
        schema="br_cgu_despesas_publicas",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2014, "end": 2031, "interval": 1},
        },
        cluster_by=["mes", "id_favorecido"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_favorecido as string) id_favorecido,
    safe_cast(nome_favorecido as string) nome_favorecido,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(nome_municipio as string) nome_municipio,
    safe_cast(id_orgao_superior as string) id_orgao_superior,
    safe_cast(nome_orgao_superior as string) nome_orgao_superior,
    safe_cast(id_orgao as string) id_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(id_unidade_gestora as string) id_unidade_gestora,
    safe_cast(nome_unidade_gestora as string) nome_unidade_gestora,
    safe_cast(valor_recebido as float64) valor_recebido,
from {{ set_datalake_project("br_cgu_despesas_publicas_staging.favorecido") }} as t
