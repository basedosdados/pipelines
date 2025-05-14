{{ config(alias="evento", schema="br_camara_dados_abertos") }}
select
    safe_cast(id as string) id_evento,
    safe_cast(urldocumentopauta as string) url_documento_pauta,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datahorainicio)), 'T')[
            offset(0)
        ] as date
    ) data_inicio,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datahorainicio)), 'T')[
            offset(1)
        ] as time
    ) horario_inicio,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datahorafim)), 'T')[
            offset(0)
        ] as date
    ) data_final,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datahorafim)), 'T')[
            offset(1)
        ] as time
    ) horario_final,
    safe_cast(situacao as string) situacao,
    safe_cast(descricao as string) descricao,
    safe_cast(descricaotipo as string) tipo,
    safe_cast(localexterno as string) local_externo,
    safe_cast(localcamara_nome as string) nome_local,
from {{ set_datalake_project("br_camara_dados_abertos_staging.evento") }} as t
