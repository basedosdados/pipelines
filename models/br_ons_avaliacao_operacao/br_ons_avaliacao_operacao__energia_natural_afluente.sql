{{
    config(
        alias="energia_natural_afluente",
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
    safe_cast(id_reservatorio as string) id_reservatorio,
    safe_cast(tipo_reservatorio as string) tipo_reservatorio,
    safe_cast(id_subsistema as string) id_subsistema,
    safe_cast(subsistema as string) subsistema,
    safe_cast(bacia as string) bacia,
    safe_cast(
        reservatorio_equivalente_energia as string
    ) reservatorio_equivalente_energia,
    safe_cast(energia_natural_afluente_bruta as float64) energia_natural_afluente_bruta,
    safe_cast(
        energia_natural_afluente_armazenavel as float64
    ) energia_natural_afluente_armazenavel,
    safe_cast(
        energia_natural_afluente_longo_termo as float64
    ) energia_natural_afluente_longo_termo,
    safe_cast(energia_natural_afluente_queda as float64) energia_natural_afluente_queda,
    safe_cast(
        proporcao_energia_natural_afluente_bruta as float64
    ) proporcao_energia_natural_afluente_bruta,
    safe_cast(
        proporcao_energia_natural_afluente_armazenavel as float64
    ) proporcao_energia_natural_afluente_armazenavel
from
    {{
        set_datalake_project(
            "br_ons_avaliacao_operacao_staging.energia_natural_afluente"
        )
    }} t
