{{
    config(
        schema="br_cgu_servidores_executivo_federal",
        alias="observacoes",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2024, "interval": 1},
        },
        cluster_by=["ano", "mes"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_servidor as string) id_servidor,
    safe_cast(nome as string) nome,
    safe_cast(cpf as string) cpf,
    safe_cast(observacao as string) observacao,
    safe_cast(origem as string) origem,
from
    {{
        set_datalake_project(
            "br_cgu_servidores_executivo_federal_staging.observacoes"
        )
    }} as t
