{{
    config(
        alias="setor_bndes_consulta",
        schema="br_bndes_estatisticas_operacoes",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1995, "end": 2031, "interval": 1},
        },
        labels={"project_id": "basedosdados"},
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(setor_bndes as string) setor_bndes,
    safe_cast(valor_consulta as float64) valor_consulta
from
    {{
        set_datalake_project(
            "br_bndes_estatisticas_operacoes_staging.setor_bndes_consulta"
        )
    }} as t
