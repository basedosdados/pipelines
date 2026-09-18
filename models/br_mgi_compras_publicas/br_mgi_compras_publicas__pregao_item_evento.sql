{{
    config(
        schema="br_mgi_compras_publicas",
        alias="pregao_item_evento",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_compra as string) id_compra,
    safe_cast(numero_item as string) numero_item,
    safe_cast(numero_grupo as string) numero_grupo,
    safe_cast(ordem_evento as int64) ordem_evento,
    safe_cast(nome_evento as string) nome_evento,
    safe_cast(
        parse_datetime('%d/%m/%Y %H:%M:%S', data_hora_evento) as datetime
    ) data_hora_evento,
    safe_cast(nome_responsavel as string) nome_responsavel,
    safe_cast(observacoes as string) observacoes
from
    {{ set_datalake_project("br_mgi_compras_publicas_staging.pregao_item_evento") }}
    as t
qualify
    row_number() over (
        partition by ano, id_compra, numero_item, ordem_evento
        order by data_hora_evento desc
    )
    = 1
