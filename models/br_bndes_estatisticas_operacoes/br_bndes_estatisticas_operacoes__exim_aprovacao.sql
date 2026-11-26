{{
    config(
        alias="exim_aprovacao",
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
    safe_cast(modalidade_exim as string) modalidade_exim,
    safe_cast(valor_aprovado as float64) valor_aprovado
from
    {{ set_datalake_project("br_bndes_estatisticas_operacoes_staging.exim_aprovacao") }}
    as t
