{{ config(alias="evento_presenca_deputado", schema="br_camara_dados_abertos") }}
select distinct
    safe_cast(idevento as string) id_evento,
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
    safe_cast(iddeputado as string) id_deputado,
from
    {{
        set_datalake_project(
            "br_camara_dados_abertos_staging.evento_presenca_deputado"
        )
    }} as t
