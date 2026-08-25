{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="suprido_transacao_objeto",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2020, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_transacao as string) id_transacao,
    safe_cast(id_objeto as string) id_objeto,
    safe_cast(descricao_objeto as string) descricao_objeto,
    safe_cast(tipo_despesa as string) tipo_despesa,
    safe_cast(subtipo_despesa as string) subtipo_despesa,
    safe_cast(rubrica as string) rubrica,
    safe_cast(quantidade as float64) quantidade,
    safe_cast(valor_unitario as float64) valor_unitario,
    safe_cast(valor_total as float64) valor_total
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.suprido_transacao_objeto"
        )
    }} as t
