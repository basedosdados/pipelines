{{ config(alias="deputado_profissao", schema="br_camara_dados_abertos") }}
select distinct
    safe_cast(id as string) id_deputado,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datahora)), 'T')[
            offset(0)
        ] as date
    ) data,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datahora)), 'T')[
            offset(1)
        ] as time
    ) horario,
    safe_cast(codtipoprofissao as string) id_profissao,
    safe_cast(titulo as string) titulo,
from
    {{ set_datalake_project("br_camara_dados_abertos_staging.deputado_profissao") }}
    as t
