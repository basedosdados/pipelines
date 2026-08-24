{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="suprido_empenho",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(codigo_ato_concessao as string) codigo_ato_concessao,
    safe_cast(numero as string) numero,
    safe_cast(rubrica as string) rubrica,
    safe_cast(descricao as string) descricao,
    safe_cast(valor_concedido as float64) valor_concedido,
    safe_cast(valor_executado as float64) valor_executado,
    safe_cast(data as date) data,
    safe_cast(data_processamento as datetime) data_processamento
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.suprido_empenho"
        )
    }} as t
