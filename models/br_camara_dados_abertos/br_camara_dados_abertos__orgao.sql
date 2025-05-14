{{ config(alias="orgao", schema="br_camara_dados_abertos") }}
select
    regexp_extract(uri, r'/orgaos/(\d+)') id_orgao,
    safe_cast(nome as string) nome,
    safe_cast(apelido as string) apelido,
    safe_cast(sigla as string) sigla,
    safe_cast(tipoorgao as string) tipo_orgao,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datainicio)), 'T')[
            offset(0)
        ] as date
    ) data_inicio,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datainstalacao)), 'T')[
            offset(0)
        ] as date
    ) data_instalacao,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datafim)), 'T')[
            offset(0)
        ] as date
    ) data_final,
    safe_cast(descricaosituacao as string) situacao,
    safe_cast(casa as string) casa,
    safe_cast(sala as string) sala,
from {{ set_datalake_project("br_camara_dados_abertos_staging.orgao") }} as t
