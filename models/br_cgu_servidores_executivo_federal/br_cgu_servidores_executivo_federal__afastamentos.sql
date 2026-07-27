{{
    config(
        schema="br_cgu_servidores_executivo_federal",
        alias="afastamentos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2024, "interval": 1},
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
    (
        case
            when data_inicio = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_inicio)
        end
    ) as data_inicio,
    (
        case
            when data_final = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_final)
        end
    ) as data_final,
    safe_cast(origem as string) origem,
from
    {{
        set_datalake_project(
            "br_cgu_servidores_executivo_federal_staging.afastamentos"
        )
    }} as t
