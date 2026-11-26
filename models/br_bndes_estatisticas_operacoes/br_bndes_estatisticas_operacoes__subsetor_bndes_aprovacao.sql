{{
    config(
        alias="subsetor_bndes_aprovacao",
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
    safe_cast(subsetor_bndes as string) subsetor_bndes,
    safe_cast(valor_aprovado as float64) valor_aprovado
from
    {{
        set_datalake_project(
            "br_bndes_estatisticas_operacoes_staging.subsetor_bndes_aprovacao"
        )
    }} as t
