{{
    config(
        alias="energia_armazenada_reservatorio",
        schema="br_ons_avaliacao_operacao",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2024, "interval": 1},
        },
        cluster_by=["ano", "mes"],
    )
}}

select
    safe_cast(data as date) data,
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(reservatorio as string) reservatorio,
    safe_cast(tipo_reservatorio as string) tipo_reservatorio,
    safe_cast(
        replace(id_reservatorio_planejamento, '.0', '') as string
    ) id_reservatorio_planejamento,
    safe_cast(
        reservatorio_equivalente_energia as string
    ) reservatorio_equivalente_energia,
    safe_cast(id_subsistema as string) id_subsistema,
    safe_cast(subsistema as string) subsistema,
    safe_cast(id_subsistema_jusante as string) id_subsistema_jusante,
    safe_cast(subsistema_jusante as string) subsistema_jusante,
    safe_cast(bacia as string) bacia,
    safe_cast(energia_armazenada_subsistema as float64) energia_armazenada_subsistema,
    safe_cast(
        energia_armazenada_jusante_subsistema as float64
    ) energia_armazenada_jusante_subsistema,
    safe_cast(
        energia_maxima_armazenada_subsistema as float64
    ) energia_maxima_armazenada_subsistema,
    safe_cast(
        energia_maxima_armazenada_jusante_subsistema as float64
    ) energia_maxima_armazenada_jusante_subsistema,
    safe_cast(energia_armazenada_total as float64) energia_armazenada_total,
    safe_cast(
        energia_maxima_armazenada_total as float64
    ) energia_maxima_armazenada_total,
    safe_cast(proporcao_energia_armazenada as float64) proporcao_energia_armazenada,
    safe_cast(
        proporcao_contribuicao_energia_armazenada_bacia as float64
    ) proporcao_contribuicao_energia_armazenada_bacia,
    safe_cast(
        proporcao_contribuicao_energia_maxima_armazenada_bacia as float64
    ) proporcao_contribuicao_energia_maxima_armazenada_bacia,
    safe_cast(
        proporcao_contribuicao_energia_armazenada_subsistema as float64
    ) proporcao_contribuicao_energia_armazenada_subsistema,
    safe_cast(
        proporcao_contribuicao_energia_maxima_armazenada_subsistema as float64
    ) proporcao_contribuicao_energia_maxima_armazenada_subsistema,
    safe_cast(
        proporcao_contribuicao_energia_armazenada_subsistema_jusante as float64
    ) proporcao_contribuicao_energia_armazenada_subsistema_jusante,
    safe_cast(
        proporcao_contribuicao_energia_maxima_armazenada_subsistema_jusante as float64
    ) proporcao_contribuicao_energia_maxima_armazenada_subsistema_jusante,
    safe_cast(
        proporcao_contribuicao_energia_armazenada_sin as float64
    ) proporcao_contribuicao_energia_armazenada_sin,
    safe_cast(
        proporcao_contribuicao_energia_armazenada_maxima_sin as float64
    ) proporcao_contribuicao_energia_armazenada_maxima_sin
from
    {{
        set_datalake_project(
            "br_ons_avaliacao_operacao_staging.energia_armazenada_reservatorio"
        )
    }} t
