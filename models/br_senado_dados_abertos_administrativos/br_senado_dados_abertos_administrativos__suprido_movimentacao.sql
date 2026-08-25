{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="suprido_movimentacao",
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
    safe_cast(id_movimentacao as string) id_movimentacao,
    safe_cast(codigo_ato_concessao as string) codigo_ato_concessao,
    safe_cast(tipo as string) tipo,
    safe_cast(numero as string) numero,
    safe_cast(tipo_inscricao as string) tipo_inscricao,
    safe_cast(inscricao as string) inscricao,
    safe_cast(fornecedor as string) fornecedor,
    safe_cast(rubricas as string) rubricas,
    safe_cast(valor as float64) valor,
    safe_cast(data as date) data,
    safe_cast(data_processamento as datetime) data_processamento
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.suprido_movimentacao"
        )
    }} as t
